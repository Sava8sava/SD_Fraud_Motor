import json
from datetime import datetime
#from kafka import KafkaConsumer, TopicPartition
from confluent_kafka import Consumer,KafkaError 
import statistics
import uuid


# Tabelas de referência para a validação geográfica
CIDADES_COORDENADAS = {
    "-23.5505,-46.6333": "Sao_Paulo",
    "-22.9068,-43.1729": "Rio_Janeiro",
    "-5.0920,-42.8038": "Teresina"
}

DISTANCIAS_E_TEMPOS = {
    "Sao_Paulo": {
        "Rio_Janeiro": {"km": 430, "tempo_minimo_minutos": 60},
        "Teresina": {"km": 2350, "tempo_minimo_minutos": 180},
    },
    "Rio_Janeiro": {
        "Sao_Paulo": {"km": 430, "tempo_minimo_minutos": 60},
        "Teresina": {"km": 2150, "tempo_minimo_minutos": 180},
    },
    "Teresina": {
        "Sao_Paulo": {"km": 2350, "tempo_minimo_minutos": 180},
        "Rio_Janeiro": {"km": 2150, "tempo_minimo_minutos": 180},
    }
}

historico_usuarios = {}

def processar_transacao(transacao):
    user_id = transacao["id_conta"]
    valor = transacao["valor"]
    local_atual_coord = transacao["localizacao"]
    
    timestamp_atual = datetime.fromisoformat(transacao["timestamp"].replace("Z", "+00:00"))
    cidade_atual = CIDADES_COORDENADAS.get(local_atual_coord, "Desconhecido")

    print(f"\n[Processando] Usuário: {user_id} | Valor: R${valor} | Local: {cidade_atual} | Offset: {transacao.get('offset', '?')}")

    # =========================================================================
    # CORREÇÃO: Carregar a memória do usuário ANTES de rodar as regras
    # =========================================================================
    if user_id not in historico_usuarios:
        historico_usuarios[user_id] = []
    
    historico_recente = historico_usuarios[user_id]

    # -------------------------------------------------------------------------
    # REGRA 1: VALOR ANÔMALO (Matemática de Outliers)
    # -------------------------------------------------------------------------
    if len(historico_recente) >= 4:
        valores_anteriores = [t["valor"] for t in historico_recente]
        media = statistics.mean(valores_anteriores)
        desvio = statistics.stdev(valores_anteriores)
        
        limite_estatistico = media + (3 * desvio)
        
        if valor > limite_estatistico and valor > 300.0:
            print(f"🚨 FRAUDE DETECTADA [Outlier Estatístico]!")
            print(f"   Detalhes: R${valor} fugiu do padrão. Média: R${round(media, 2)} | Limite Dinâmico: R${round(limite_estatistico, 2)}")
            return "BLOQUEADO_VALOR"
    else:
        # Fallback para usuários novos sem histórico suficiente
        if valor > 2000.0:
            print(f"🚨 FRAUDE DETECTADA [Valor Anômalo]: Compra de R${valor} excede o limite inicial permitido!")
            return "BLOQUEADO_VALOR"

    # -------------------------------------------------------------------------
    # REGRA 2: LOCALIZAÇÃO IMPOSSÍVEL (Geolocalização)
    # -------------------------------------------------------------------------
    if len(historico_recente) > 0:
        ultima_transacao = historico_recente[-1]
        local_antigo_coord = ultima_transacao["localizacao"]
        timestamp_antigo = datetime.fromisoformat(ultima_transacao["timestamp"].replace("Z", "+00:00"))
        cidade_antiga = CIDADES_COORDENADAS.get(local_antigo_coord, "Desconhecido")

        if cidade_atual != cidade_antiga and cidade_atual != "Desconhecido" and cidade_antiga != "Desconhecido":
            tempo_decorrido_segundos = (timestamp_atual - timestamp_antigo).total_seconds()
            tempo_decorrido_minutos = tempo_decorrido_segundos / 60.0
            
            dados_viagem = DISTANCIAS_E_TEMPOS[cidade_antiga][cidade_atual]
            tempo_minimo_exigido = dados_viagem["tempo_minimo_minutos"]
            distancia_km = dados_viagem["km"]

            if tempo_decorrido_minutos < tempo_minimo_exigido:
                print(f"🚨 FRAUDE DETECTADA [Localização Impossível]!")
                print(f"   Detalhes: O usuário estava em {cidade_antiga} e apareceu em {cidade_atual} ({distancia_km} km) em apenas {round(tempo_decorrido_segundos, 2)} segundos!")
                return "BLOQUEADO_GEOLOCALIZACAO"

    # -------------------------------------------------------------------------
    # REGRA 3: ALTA FREQUÊNCIA (Rajada de compras)
    # -------------------------------------------------------------------------
    transacoes_ultimos_10s = [
        t for t in historico_recente 
        if (timestamp_atual - datetime.fromisoformat(t["timestamp"].replace("Z", "+00:00"))).total_seconds() <= 10
    ]
    
    if len(transacoes_ultimos_10s) >= 4:
        print(f"🚨 FRAUDE DETECTADA [Alta Frequência]: {len(transacoes_ultimos_10s) + 1} compras tentadas em menos de 10 segundos!")
        return "BLOQUEADO_FREQUENCIA"

    # =========================================================================
    # SUCESSO: Se passou por tudo, adiciona a transação legítima no cache
    # =========================================================================
    historico_usuarios[user_id].append(transacao)
    
    # Mantém a memória leve limitando a 20 registros
    if len(historico_usuarios[user_id]) > 20:
        historico_usuarios[user_id].pop(0)

    print("✅ Transação Aprovada com Sucesso.")
    return "APROVADO"

def main():
    print("Iniciando consumer...")

    conf = {
        'bootstrap.servers': 'broker-kafka:29094',
        'group.id': 'motor-antifraude-confluent-v1',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,  # Commit manual
        # Parâmetros de resiliência nativos da librdkafka
        'session.timeout.ms': 45000,
        'max.poll.interval.ms': 300000
    }
    consumer = Consumer(conf)
    consumer.subscribe(['transacoes_pendentes'])
    print("Assignment:", consumer.assignment())

    try:
        while True:
            mensagens = consumer.poll(timeout=1.0)

            if not mensagens:
                continue
            if mensagens.error():
                if mensagens.error().code() == KafkaError._PARTITION_EOF:
                    continue 
                else:
                    print(f"ERRO_KAFKA:{mensagens.error()}")
                    break

            transacao = json.loads(mensagens.value().decode('utf-8'))
            transacao['offset'] = mensagens.offset()

            # Executa sua lógica matemática impecável
            resultado = processar_transacao(transacao)
            print("Resultado:", resultado)

            # Commit manual idêntico ao que você queria fazer
            consumer.commit(asynchronous=False)

    except KeyboardInterrupt:
        print("Encerrado")

    finally:
        consumer.close()

if __name__ == "__main__":
    main()

import json
import uuid
import time 
from datetime import datetime, timezone
#from kafka import KafkaProducer
from confluent_kafka import Producer
import random 

#estrutura 
'''
"Id_Trasação" : string uuid
"id_conta" : string/int
"valor" : float
"localzação" : string ou lat/long
"timestamp" : string(iso 8601 ou epoch)
'''

#variaveis 
#id do usuario : cidade natal dele
CIDADES = {
    "Sao_Paulo": "-23.5505,-46.6333",
    "Rio_Janeiro": "-22.9068,-43.1729",
    "Teresina": "-5.0920,-42.8038",
}

user_local_list= {
        "usuario_000" : "Sao_Paulo",
        "usuario_067" : "Teresina",
        "usuario_011" : "Sao_Paulo",
        "usuario_069" : "Sao_Paulo",
        "usuario_666" : "Rio_Janeiro"
}

# Distâncias aproximadas em KM e tempo mínimo estimado em minutos
DISTANCIAS_E_TEMPOS = {
    "Sao_Paulo": {
        "Rio_Janeiro": {"km": 430, "tempo_minimo_minutos": 60},     # 1h de voo ou ~5h de carro
        "Teresina": {"km": 2350, "tempo_minimo_minutos": 180},     # ~3h de voo comercial
    },
    "Rio_Janeiro": {
        "Sao_Paulo": {"km": 430, "tempo_minimo_minutos": 60},
        "Teresina": {"km": 2150, "tempo_minimo_minutos": 180},     # ~3h de voo comercial
    },
    "Teresina": {
        "Sao_Paulo": {"km": 2350, "tempo_minimo_minutos": 180},
        "Rio_Janeiro": {"km": 2150, "tempo_minimo_minutos": 180},
    }
}
conter = 0 

conf = {
    'bootstrap.servers': 'broker-kafka:29094',
    'client.id': 'app-gerador-producer',
    'acks': 'all' }

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Falha ao entregar mensagem: {err}")
    else:
        print(f"🔹 Tópico: {msg.topic()} | Partição: {msg.partition()} | Offset: {msg.offset()}")


def generate_transaction(client, cidade_compra= None, value_range = [10.0,500.0]):
    #caso o usuario não informe o lugar da compra então é na cidade natal 
    if cidade_compra is None:
        cidade_compra = user_local_list[client]

    trasacao = {
        "Id_Trasacao": str(uuid.uuid4()),
        "id_conta": client,
        "valor": round(random.uniform(value_range[0],value_range[1]), 2),
        "localizacao": CIDADES[cidade_compra], 
        "timestamp" : datetime.now(timezone.utc).isoformat()
    }
    return trasacao


def simulate_high_frequency_scam(client):
    lista_transacoes_fraude = []
    frequence = random.randint(8,15)
    for _ in range(frequence):
        nova_t = generate_transaction(client)
        lista_transacoes_fraude.append(nova_t)

    return lista_transacoes_fraude 

def simulate_impossible_location_scam(client):
    lista_transacoes_fraude = []
    
    # 1. Pega a cidade natal do usuário para fazer a primeira compra legítima
    cidade_natal = user_local_list[client]
    t1 = generate_transaction(client, cidade_compra=cidade_natal)
    lista_transacoes_fraude.append(t1)
    
    # 2. Escolhe uma cidade destino diferente da natal para a fraude
    cidades_disponiveis = [c for c in CIDADES.keys() if c != cidade_natal]
    cidade_fraude = random.choice(cidades_disponiveis)
    
    # Força uma pequena pausa simulando milissegundos de diferença
    time.sleep(0.1) 
    
    # 3. Gera a segunda transação na cidade impossível
    t2 = generate_transaction(client, cidade_compra=cidade_fraude)
    lista_transacoes_fraude.append(t2)
    
    return lista_transacoes_fraude

def simulate_abnormal_value_purchase(client):
    t1 = generate_transaction(client,value_range=[1800,5000])
    return [t1] 

def main(user_dict : dict):
    cont = 0 
    users_ids = list(user_dict.keys())
    try:
        while True:
            cont += 1
            random_user = random.choice(users_ids)
            transaction_buffer = []

            event_chance = random.randint(1,100)
            
            #valor anomalo 2%
            if event_chance <= 2:
                print(f"Simulando evento de valor anomalo na transação do usuario : {random_user}!\n")
                transaction_buffer = simulate_abnormal_value_purchase(random_user)
                
            # geolocalização impossível 2%
            elif event_chance <= 4:
                print(f"Simulando evento de localização impossível na transação do usuario : {random_user}!\n")
                transaction_buffer = simulate_impossible_location_scam(random_user)
            # varias compras no espaço de tempo muto curto
            elif event_chance <= 7:
                print(f"Simulando evento de alta frequencia na transação do usuario : {random_user}!\n")
                transaction_buffer = simulate_high_frequency_scam(random_user)
            
            else:
                normal_trasaction = generate_transaction(random_user)
                transaction_buffer = [normal_trasaction]

            for transactions in transaction_buffer:
                payload = json.dumps(transactions).encode('utf-8')
                print(f"🔹 [Total Envios: {cont}] ", end="")
                
                producer.produce(
                    'transacoes_pendentes', 
                    value=payload, 
                    callback=delivery_report
                )
                producer.poll(0)

                if event_chance > 7:
                    time.sleep(0.1)
            
            producer.flush()
            time.sleep(2)
    except KeyboardInterrupt:
        print("Simulação Encerrada Manualmente!\n")
    except Exception as e:
        print(f"Erro inesperado: {e}")
    finally:
        producer.close()
        print("Conexão com o Kafka encerrada por segurança")




if __name__ == "__main__":
    main(user_local_list)

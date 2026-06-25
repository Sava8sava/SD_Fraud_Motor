# Requisitos
## Funcionais
1. O Sistema deve ser capaz de identicar possiveis transações fraudulentas
2. O Sistema deve ser capaz de notificar o responsavel de segurança em casos de transações suspeitas
3. O sistema deve ser capaz de bloquear possiveis transações fraudulentas
4. O sistema deve ser capaz de Gerar quantidades volumosas de Transações simuladas 
5. O sistema deve ser capaz de Gerar transações fraudulentas entre as trasações normais 

### Não Funcionais
1. O Sistema deve conseguir lidar com um grande volume de transações em formato Json
2. O sistema deve ser leve e modular 
3. O sistema deve ser construindo em python e rodar em Docker 
4. estruturado em Nós distribuidos(gerador, processador, e monitor) que se comunicam pela rede
5. O gerador deve simular um gateway de pagamento que envia trasações para o processador **Pendentes** para o processador analisar
6. O gerador deve rodar num loop infinito com um delay de 1 segundo que pode ser ajustavel
7. configurar ma logica que gera operações suspeitas como multiplas compras num espaço muito curto de tempo ou compras geograficamente distantes num espaço impossivel de tempo 
8. O Processador deve usar regras definidas pela equipe de segurança(por enquanto 3)
9. Se o Processador chegar a conclusão que o processo é uma fraude ele deve bloquer a trasação e publicar ela como transação fraudulenta e se legitima apenas avisar que é legtima  
10. O monitor deve ser ativado apenas nó caso de possiveis trasações fraudulentas ele deve imprime notificações de alertas 
11. Os tópicos do Apache Kafka devem utilizar volumes do Docker para garantir que as mensagens não sejam perdidas caso os containers sejam reiniciados.
12. Todos os três nós (gerador, processador, monitor) e o broker Kafka devem rodar na mesma docker network para comunicação interna via hostname.
13. processador deve garantir o processamento do tipo At-least-once (Pelo menos uma vez) confirmando o recebimento da mensagem (commit) apenas após processar a regra de fraude.

### Stack
- Docker
- Kafka apache(docker composer)
- Python 
    - Kafka-Python

### Formato Json
"Id_Trasação" : string uuid
"id_conta" : string/int
"valor" : float
"localzação" : string ou lat/long
"timestamp" : string(iso 8601 ou epoch)

## Comentarios 
- devido a condição de ser um cenário simulado para um trabalho acadêmico, o estado temporário será mantido em memória volátil pela simpliciade do projeto
- Por usar um sistema simples de identificação de fraude baseada em if/else, certas regras serão simplificadas


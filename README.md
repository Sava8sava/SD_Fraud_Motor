**Para reiniciar do zero** (offset volta para o início):
```bash
sudo docker compose down -v
sudo docker compose up --build
```
O `-v` apaga o volume do Kafka, então perde tudo — offsets, mensagens, histórico.

**Para só reiniciar os containers** (mantém o offset de onde parou):
```bash
sudo docker compose down
sudo docker compose up
```
Sem o `-v` o volume persiste, então o consumer com `auto_offset_reset='latest'` continua de onde parou.

**Para só ver os logs sem reiniciar nada:**
```bash
sudo docker compose logs -f
# ou de um container específico
sudo docker logs -f app-motor-antifraude
```

**Para subir depois de mudar só o código Python** (sem mexer no Kafka):
```bash
sudo docker compose up --build
```
O `--build` reconstrói a imagem com o código novo mas não apaga o volume.

Então no seu fluxo de desenvolvimento o mais comum vai ser:

```bash
# mudou código → só rebuild
sudo docker compose up --build

# quer começar do zero → desce com -v
sudo docker compose down -v && sudo docker compose up --build
```

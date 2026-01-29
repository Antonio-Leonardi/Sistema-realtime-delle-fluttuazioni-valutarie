# Sistema-realtime-delle-fluttuazioni-valutarie

Questo progetto implementa una pipeline di streaming dati che:

1. Recupera i tassi di cambio di valute a partire dall'API FXRates.
2. Li invia in tempo reale a **kafka**.
3. Li arricchisce e li elabora con **Spark Structured Streaming**.
4. Li indicizza in **Elasticsearch**.
5. Li visualizza in una dashboard **Kibana**.

# Obiettivo
L'obiettivo è quello di acquisire i dati in modo continuo per calcolare le variazioni dei tassi di cambio di valuta, e salvati per consentire analisi in tempo reale e confronti nel tempo, a professionisti e aziende che operano in contesti internazionali che hanno la necessità di conoscere in tempo reale le variazioni dei tassi di cambio per prendere decisioni rapide.

## Tecnologie utilizzate

1. Logstash 
2. Apache Kafka (kRaft mode)
3. Apache Spark
4. Elasticsearch
5. Kibana
6. Docker e Docker Compose

# Come avviare il progetto

Assicurati di aver installato:

1. Docker
2. Docker Compose

Successivamente clona il repository:

```bash

git clone https://github.com/Antonio-Leonardi/sistema_realtime_fluttuazioni_valutarie.git

```

Cambia directory:

```bash

cd sistema_realtime_fluttuazioni_valutarie

```

```bash

cd Project

```

Avvia tutti i servizi:

```bash
docker-compose up --build
```

# Dashboard Kibana

Una volta che **Spark** inizia a inviare i dati a Elasticsearch, puoi:

1. Aprire kibana in
   
   ```text
   http://localhost:5601/app/home#/
   ```
2. Visualizzare i dati nelle dashboard
3.  Creare grafici e dashboard personalizzate

# Struttura della repository

```text
.
├── README.md
├── sistema_realtime_fluttuazioni_valutarie/
│   ├── project/
│   │   ├── docker-compose.yml
│   │   ├── logstash.conf
│   │   └── spark/
│   │       ├── Dockerfile
│   │       └── app_streaming.py
|           └── data.csv
│   ├── kibana.ndjson
│   └── presentazione.pdf

```

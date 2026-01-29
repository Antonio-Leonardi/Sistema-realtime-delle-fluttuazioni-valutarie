FROM apache/spark:3.4.2

USER root

RUN pip install requests

# Crea la directory Ivy e assegna i permessi
RUN mkdir -p /home/spark/.ivy2/cache \
    && chown -R spark:spark /home/spark/.ivy2

USER spark

WORKDIR /opt/spark-app

COPY app_streaming.py .

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[*]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2", \
     "app_streaming.py"]

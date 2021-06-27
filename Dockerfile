FROM openjdk:11
ENV PYSPARK_ARGS="--packages io.delta:delta-core_2.12:1.0.0 --conf \"spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension\" --conf \"spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog\""
VOLUME /data
WORKDIR /app
COPY ./python/requirements.txt requirements.txt
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install -r requirements.txt && \
    echo "print('dummy command for delta libs download')" > dummy.py && \
    /usr/local/bin/spark-submit ${PYSPARK_ARGS} dummy.py && \
    rm dummy.py
COPY ./python/delta_api/delta_api api
CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "-b", "0.0.0.0:8000", "api.api_app:app"]
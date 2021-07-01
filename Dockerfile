FROM openjdk:11
ENV PYSPARK_ARGS="--packages io.delta:delta-core_2.12:1.0.0"
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
CMD ["gunicorn", "--worker-class", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000", "--log-level", "debug", "api.api_app:app"]
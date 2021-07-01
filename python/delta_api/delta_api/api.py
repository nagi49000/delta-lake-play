import json
import logging
from typing import List
from fastapi import FastAPI
from fastapi.logger import logger
from pydantic import BaseModel
from .spark_project import get_spark
from .spark_project import get_or_create_names_table


class HelloWorldResponse(BaseModel):
    message: str


class GetTableRequest(BaseModel):
    version: int


class TableRow(BaseModel):
    id: int
    firstname: str
    lastname: str


class MergeToTableRequest(BaseModel):
    data: List[TableRow]


class GetTableResponse(BaseModel):
    version: int
    data: List[TableRow]


class DeleteFromTableRequest(BaseModel):
    ids: List[int]


def create_app(delta_dir):
    # hijack the gunicorn logger so that we can use it for logging
    gunicorn_logger = logging.getLogger('gunicorn.error')
    logger.handlers = gunicorn_logger.handlers
    logger.setLevel(gunicorn_logger.level)

    spark = get_spark()
    app = FastAPI()
    names_table, names_table_location = get_or_create_names_table(spark, delta_dir)

    @app.get("/hello_world", response_model=HelloWorldResponse)
    async def hello_world():
        logger.debug("/hello_world")
        return {"message": "Hello World"}

    @app.get("/get_table_history")
    async def get_table_history():
        logger.debug("/get_table_history")
        df = names_table.history().toPandas()
        # make timestamps human readable
        df["timestamp"] = df["timestamp"].astype(str)
        # convert to json and back to make a json compliant dict
        return json.loads(df.set_index("version").to_json())

    @app.post("/get_table", response_model=GetTableResponse)
    async def get_table(r: GetTableRequest):
        logger.debug("/get_table")
        latest_version = names_table.history().agg({"version": "max"}).collect()[0][0]
        if 0 <= r.version <= latest_version:
            sdf = spark.read.format("delta").option("versionAsOf", r.version).load(names_table_location)
            version = r.version
        else:  # get latest version
            sdf = names_table.toDF()
            version = latest_version
        df = sdf.toPandas()
        return {"version": version, "data": df.to_dict(orient="records")}

    @app.put("/merge_to_table")
    async def merge_to_table(r: MergeToTableRequest):
        logger.debug("/merge_to_table")
        sdf = spark.createDataFrame(r.data)
        names_table.alias("names").merge(
            source=sdf.alias("updates"),
            condition="names.id = updates.id"
        ).whenMatchedUpdate(set={
            "firstname": "updates.firstname",
            "lastname": "updates.lastname"
        }).whenNotMatchedInsert(values={
            "id": "updates.id",
            "firstname": "updates.firstname",
            "lastname": "updates.lastname"
        }).execute()

    @app.delete("/delete_from_table")
    async def delete_from_table(r: DeleteFromTableRequest):
        logger.debug("/delete_from_table")
        names_table.delete(f"id IN {tuple(r.ids)}")

    return app

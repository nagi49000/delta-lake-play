import json
import logging
import datetime
from typing import (
    List,
    Union
)
from py4j.protocol import Py4JJavaError
from fastapi import (
    FastAPI,
    HTTPException
)
from fastapi.logger import logger
from pydantic import BaseModel
from .spark_project import get_spark
from .spark_project import get_or_create_names_table


class HelloWorldResponse(BaseModel):
    message: str


class TableRow(BaseModel):
    id: int
    firstname: str
    lastname: str


class MergeToTableRequest(BaseModel):
    data: List[TableRow]


class DeleteFromTableRequest(BaseModel):
    ids: List[int]


class GetTableRequest(BaseModel):
    version: Union[int, datetime.datetime] = None  # default to latest


class GetTableResponse(GetTableRequest):
    data: List[TableRow]


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
        try:
            if r.version is None:
                sdf = names_table.toDF()
                version = names_table.history().agg({"version": "max"}).collect()[0][0]
            elif isinstance(r.version, int):
                sdf = spark.read.format("delta").option("versionAsOf", r.version).load(names_table_location)
                version = r.version
            else:  # assume datetime
                sdf = spark.read.format("delta").option("timestampAsOf", r.version).load(names_table_location)
                version = r.version
        except Py4JJavaError as e:
            logger.error(f"/get_table: {e}")
            raise HTTPException(status_code=500, detail=str(e).split("\n\tat")[0])  # split of Java traceback
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

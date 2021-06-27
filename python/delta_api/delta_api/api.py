import logging
from fastapi import FastAPI
from pydantic import BaseModel
from .spark_project import get_spark
from .spark_project import get_or_create_names_table


class HelloWorldResponse(BaseModel):
    message: str


def create_app(delta_dir):
    spark = get_spark()
    app = FastAPI()
    names_table = get_or_create_names_table(spark, delta_dir)

    @app.get("/hello_world", response_model=HelloWorldResponse)
    async def hello_world():
        logging.debug('/hello_world')
        return {"message": "Hello World"}

    return app

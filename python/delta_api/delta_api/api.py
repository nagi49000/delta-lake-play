from fastapi import FastAPI
from pydantic import BaseModel
from .spark_project import get_spark


class HelloWorldResponse(BaseModel):
    message: str


def create_app():
    spark = get_spark()
    app = FastAPI()

    @app.get("/hello_world", response_model=HelloWorldResponse)
    async def hello_world():
        logging.debug('/hello_world')
        return {"message": "Hello World"}

    return app

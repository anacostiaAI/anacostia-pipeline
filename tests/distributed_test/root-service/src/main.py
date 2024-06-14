from typing import Union
import logging
import shutil
import os
from fastapi import FastAPI

app = FastAPI()

root_test_path = "/testing_artifacts"

log_path = f"{root_test_path}/anacostia.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='ROOT %(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='a'
)
logger = logging.getLogger(__name__)



@app.get("/")
def read_root():
    logger.debug("hello from root service")
    return {"Hello": "Root World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}
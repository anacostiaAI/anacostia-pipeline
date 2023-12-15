from logging import Logger
import os
import time
import logging
import shutil
from typing import List, Union
import requests

from dotenv import load_dotenv
from anacostia_pipeline.metadata.sql_metadata_store import SqliteMetadataStore



sqlite_tests_path = "./testing_artifacts/sqlite_tests"
if os.path.exists(sqlite_tests_path) is True:
    shutil.rmtree(sqlite_tests_path)

os.makedirs(sqlite_tests_path)
log_path = f"{sqlite_tests_path}/anacostia.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='w'
)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    sqlite_store = SqliteMetadataStore("test", f"sqlite:///{sqlite_tests_path}/test.db")
    sqlite_store.setup()
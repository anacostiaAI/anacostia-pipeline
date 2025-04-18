import os
import logging
import shutil
from typing import List

from anacostia_pipeline.nodes.metadata.sql.sqlite.node import SQLiteMetadataStoreNode


tests_path = "./testing_artifacts/sqlalchemy_tests"
if os.path.exists(tests_path) is True:
    shutil.rmtree(tests_path)
os.makedirs(tests_path)
log_path = f"{tests_path}/anacostia.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='a'
)
logger = logging.getLogger(__name__)

metadata_store_path = f"{tests_path}/metadata_store"
data_store_path = f"{tests_path}/data_store"


metadata_store = SQLiteMetadataStoreNode(
    name="metadata_store",
    uri=f"sqlite:///{metadata_store_path}/metadata.db",
    loggers=[logger]
)
import os
import shutil
import logging
import json
from anacostia_pipeline.metadata.sql_metadata_store import SqliteMetadataStore


metadata_tests_path = "./testing_artifacts/metadata"
if os.path.exists(metadata_tests_path) is True:
    shutil.rmtree(metadata_tests_path)
os.makedirs(metadata_tests_path)

log_path = f"{metadata_tests_path}/anacostia.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='a'
)
logger = logging.getLogger(__name__)


path = f"{metadata_tests_path}/metadata_test"
metadata_store_path = f"{path}/metadata_store"

metadata_store = SqliteMetadataStore(
    name="metadata_store", 
    uri=f"sqlite:///{metadata_store_path}/metadata.db"
)


if __name__ == "__main__":
    metadata_store.setup()
    metadata_store.start_run()
    runs = metadata_store.get_runs_json(path=f"{path}/data.json")
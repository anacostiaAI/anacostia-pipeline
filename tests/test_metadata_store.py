from typing import Any
import unittest
import logging
import sys
import os
import shutil
import random

sys.path.append('..')
sys.path.append('../anacostia_pipeline')
from anacostia_pipeline.resource.metadata_store import MetadataStoreNode
from anacostia_pipeline.engine.pipeline import Pipeline

from test_utils import *


# Set the seed for reproducibility
seed_value = 42
random.seed(seed_value)

metadata_store_tests_path = "./testing_artifacts/metadata_store_tests"
if os.path.exists(metadata_store_tests_path) is True:
    shutil.rmtree(metadata_store_tests_path)

os.makedirs(metadata_store_tests_path)
os.chmod(metadata_store_tests_path, 0o777)

# Create a logger
log_path = f"{metadata_store_tests_path}/metadata_store.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='w'
)
logger = logging.getLogger(__name__)


class MetadataStoreTests(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)

    def setUp(self) -> None:
        self.path = f"{metadata_store_tests_path}/{self._testMethodName}"
        os.makedirs(self.path)

    def test_empty_setup(self):
        metadata_store = MetadataStoreNode(name="metadata_store", metadata_store_path=self.path, init_state="current")
        pipeline = Pipeline(nodes=[metadata_store], logger=logger)
        pipeline.start()
        
        time.sleep(1)
        for _ in range(5):
            acc = random.randint(0, 100) / 100
            auc = random.randint(0, 100) / 100
            metadata_store.insert_metadata(acc=acc, auc=auc)
        time.sleep(1)

        pipeline.terminate_nodes()

    def test_nonempty_setup(self):
        metadata_store = MetadataStoreNode(
            name="metadata_store", 
            metadata_store_path=self.path,
            init_state="current",
            init_data=[
                {"acc": 0.1, "auc": 0.2},
                {"acc": 0.3, "auc": 0.4},
                {"acc": 0.5, "auc": 0.6},
                {"acc": 0.7, "auc": 0.8},
                {"acc": 0.9, "auc": 1.0},
            ] 
        )
        pipeline = Pipeline(nodes=[metadata_store], logger=logger)
        pipeline.start()
        
        for _ in range(5):
            acc = random.randint(0, 100) / 100
            auc = random.randint(0, 100) / 100
            metadata_store.insert_metadata(acc=acc, auc=auc)
        time.sleep(1)

        pipeline.terminate_nodes()


if __name__ == "__main__":
    unittest.main()
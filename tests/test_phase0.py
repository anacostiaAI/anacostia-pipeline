from typing import Any
import unittest
import logging
import sys
import os
import shutil
import random

sys.path.append('..')
sys.path.append('../anacostia_pipeline')
from anacostia_pipeline.resource.data_store import DataStoreNode
from anacostia_pipeline.engine.node import ActionNode
from anacostia_pipeline.engine.pipeline import Pipeline

from test_utils import *


# Set the seed for reproducibility
seed_value = 42
random.seed(seed_value)

systems_tests_path = "./testing_artifacts/system_tests"
if os.path.exists(systems_tests_path) is True:
    shutil.rmtree(systems_tests_path)

os.makedirs(systems_tests_path)
os.chmod(systems_tests_path, 0o777)

# Create a logger
log_path = f"{systems_tests_path}/phase0.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='w'
)
logger = logging.getLogger(__name__)


class FileStoreNode(DataStoreNode):
    def __init__(self, name: str, path: str) -> None:
        super().__init__(name, path)
    
    def save_data_sample(self, content: str) -> None:
        filename = self.create_filename("txt")
        create_file(f"{self.data_store_path}/{filename}", content)
        self.log(f"saved data sample {filename} to data store {self.data_store_path}")
    
    def load_data_sample(self, filepath: str) -> Any:
        with open(filepath, "r") as file:
            return file.read()

    def trigger_condition(self) -> bool:
        num_new_files = self.get_num_data_samples("new")
        if num_new_files >= 5:
            return True
        else:
            return False

class ETLNode(ActionNode):
    def __init__(self, name: str, data_store: DataStoreNode) -> None:
        super().__init__(name, "ETL", listen_to=[data_store])
        self.data_store = data_store
    
    def setup(self) -> None:
        self.log(f"Setting up node '{self.name}'")
        time.sleep(4)
        self.log(f"Node '{self.name}' setup complete")

    def execute(self) -> None:
        self.log(f"Node '{self.name}' triggered")

        for path, sample in zip(self.data_store.load_data_paths("current"), self.data_store.load_data_samples("current")):
            self.log(f"processing data sample {path}")

        self.log(f"Node '{self.name}' finished")
        return True

    def on_exit(self):
        self.log(f"Node '{self.name}' exited")


class ETLTests(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)

    def setUp(self) -> None:
        self.path = f"{systems_tests_path}/{self._testMethodName}"
        os.makedirs(self.path)
    
    def test_empty_setup(self):
        data_store_node = FileStoreNode(name=f"data store {self._testMethodName}", path=self.path)
        etl_node = ETLNode(name=f"ETL {self._testMethodName}", data_store=data_store_node)
        pipeline_phase0 = Pipeline(nodes=[data_store_node, etl_node], logger=logger)
        pipeline_phase0.start()

        self.assertEqual(0, data_store_node.num_predecessors)
        self.assertEqual(1, data_store_node.num_successors)
        
        time.sleep(3)

        for i in range(8):
            data_store_node.save_data_sample(content=f"test {i+1}")
        
        time.sleep(3)

        for i in range(6):
            data_store_node.save_data_sample(content=f"test {i+1}")
        
        pipeline_phase0.terminate_nodes()


if __name__ == "__main__":
    unittest.main()

    """
    path = f"{systems_tests_path}/test_empty_setup"
    data_store_node = FileStoreNode(name="data store", path=path)
    etl_node = ETLNode(name="ETL", data_store=data_store_node)
    #data_store_node.num_successors = 1
    #etl_node.num_predecessors = 1

    pipeline = Pipeline(nodes=[data_store_node, etl_node], logger=logger)
    #pipeline = Pipeline(nodes=[data_store_node], logger=logger)
    pipeline.start()

    time.sleep(3)
    for i in range(5):
        data_store_node.save_data_sample(content=f"test {i+1}")
    time.sleep(3)
    print("terminating nodes")
    
    pipeline.terminate_nodes()
    """
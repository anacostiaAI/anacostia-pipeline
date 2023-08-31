from typing import Any, List
import unittest
import logging
import sys
import os
import shutil
import random
from threading import Thread

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


class ETLNode(ActionNode):
    def __init__(self, name: str, data_store: DataStoreNode) -> None:
        super().__init__(name, "ETL", listen_to=[data_store])
        self.data_store = data_store
    
    def setup(self) -> None:
        self.log(f"Setting up node '{self.name}'")
        time.sleep(4)
        self.log(f"Node '{self.name}' setup complete")

    def execute(self) -> None:
        self.log("ETL triggered")
        for i, sample in enumerate(self.data_store.load_data_samples("current")):
            self.log(f"processing data sample {i}")
        time.sleep(0.1)
        self.log("ETL finished")
        return True

    def update_state(self):
        self.log("updating state")
        self.data_store.event.set()
        self.log("state updated")
        self.trigger()
        self.send_signals(Status.SUCCESS)

    def on_exit(self):
        self.log("ETL node exited")

class ETLTests(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)

    def setUp(self) -> None:
        self.path = f"{systems_tests_path}/{self._testMethodName}"
        os.makedirs(self.path)
    
    def start_nodes(self, data_store_node: FileStoreNode, etl_node: ETLNode) -> None:
        data_store_node.set_logger(logger)
        etl_node.set_logger(logger)

        # we must set the number of successors and predecessors manually because we are not using a pipeline
        # etl_node.num_predecessors is set to 1 because the data store node is a predecessor
        # data_store_node.num_successors is set to 1 because the etl node is a successor
        # the number of predecessors and successors must be set once the DAG is constructed, not when the node is initialized
        data_store_node.num_successors = 1
        etl_node.num_predecessors = 1
        self.pipeline = Pipeline(nodes=[data_store_node, etl_node], logger=logger)

    def tearDown_nodes(self, data_store_node: FileStoreNode, etl_node: ETLNode) -> None:
        #etl_node.stop()
        #data_store_node.stop()
        #etl_node.join()
        #data_store_node.join()
        self.pipeline.terminate_nodes()
    
    def test_initial_setup(self):
        data_store_node = FileStoreNode(name=f"data store {self._testMethodName}", path=self.path)
        etl_node = ETLNode(name=f"ETL {self._testMethodName}", data_store=data_store_node)
        self.start_nodes(data_store_node, etl_node)

        time.sleep(1)
        self.assertEqual(1, etl_node.num_predecessors)
        self.assertEqual(0, etl_node.num_successors)
        self.assertEqual(0, data_store_node.num_predecessors)
        self.assertEqual(1, data_store_node.num_successors)
        time.sleep(1)

        self.tearDown_nodes(data_store_node, etl_node)

    def test_empty_setup(self):
        data_store_node = FileStoreNode(name=f"data store {self._testMethodName}", path=self.path)
        etl_node = ETLNode(name=f"ETL {self._testMethodName}", data_store=data_store_node)
        #self.start_nodes(data_store_node, etl_node)
        pipeline = Pipeline(nodes=[data_store_node, etl_node], logger=logger)
        pipeline.start()
        
        for i in range(5):
            data_store_node.save_data_sample(content=f"test {i+1}")
        
        pipeline.terminate_nodes()
        #self.tearDown_nodes(data_store_node, etl_node)

if __name__ == "__main__":
    #unittest.main()

    path = f"{systems_tests_path}/test_empty_setup"
    data_store_node = FileStoreNode(name="data store", path=path)
    etl_node = ETLNode(name="ETL", data_store=data_store_node)
    data_store_node.num_successors = 1
    etl_node.num_predecessors = 1

    pipeline = Pipeline(nodes=[data_store_node, etl_node], logger=logger)
    #pipeline = Pipeline(nodes=[data_store_node], logger=logger)
    pipeline.start()

    time.sleep(3)
    for i in range(5):
        data_store_node.save_data_sample(content=f"test {i+1}")
    time.sleep(3)
    print("terminating nodes")
    
    pipeline.terminate_nodes()
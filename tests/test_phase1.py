from typing import Any
import unittest
import logging
import sys
import os
import shutil
import random
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
load_dotenv()
import base64

sys.path.append('..')
sys.path.append('../anacostia_pipeline')
from anacostia_pipeline.resource.data_store import DataStoreNode
from anacostia_pipeline.resource.feature_store import FeatureStoreNode
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
log_path = f"{systems_tests_path}/phase1.log"
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

class FireflyClient:
    def __init__(self) -> None:
        self.base_url = "https://u0khg0jvam-u0eaud4c02-firefly-os.us0-aws-ws.kaleido.io/api/v1"
        self.username = os.getenv("USERNAME")
        self.password = os.getenv("PASSWORD")
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(self.username, self.password)
        self.credentials = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
        self.headers = {
            "Authorization": f"Basic {self.credentials}",
            "Content-Type": "application/json"
        }

    # Messaging API
    def broadcast_message(self, message, metadata=None):
        payload = {
            "data": [
                {
                    "value": message
                }
            ]
        }
        response = requests.post(f"{self.base_url}/messages/broadcast", json=payload, headers=self.headers)
        return response.json()

class ETLNode(ActionNode):
    def __init__(self, name: str, data_store: DataStoreNode, feature_store: FeatureStoreNode) -> None:
        super().__init__(name, "ETL", listen_to=[data_store])
        self.data_store = data_store
        self.feature_store = feature_store
        #self.client = FireflyClient()
    
    def setup(self) -> None:
        self.log(f"Setting up node '{self.name}'")
        time.sleep(4)
        self.log(f"Node '{self.name}' setup complete")
        #response = self.client.broadcast_message(f"Node '{self.name}' setup complete")

    def execute(self) -> None:
        self.log(f"Node '{self.name}' triggered")
        #response = self.client.broadcast_message(f"Node '{self.name}' triggered")

        try:
            for path, sample in zip(self.data_store.load_data_paths("current"), self.data_store.load_data_samples("current")):
                self.log(f"processing data sample {path}")
                feature_vector_filepath = self.feature_store.create_filename()
                random_number = random.randint(0, 100)
                create_numpy_file(
                    file_path=f"{self.feature_store.feature_store_path}/{feature_vector_filepath}", shape=(random_number, 3)
                ) 
            self.log(f"Node '{self.name}' execution complete")
            #response = self.client.broadcast_message(f"Node '{self.name}' execution complete")
            #response = self.client.broadcast_message("ETL complete", metadata={"node": self.name})
            #print(response)
            return True

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.log(f"Error processing data sample: {e}, exc_type: {exc_type}, fname: {fname}, exc_tb.tb_lineno: {exc_tb.tb_lineno}")
            #response = self.client.broadcast_message(f"Error processing data sample: {e}")
            return False

    def on_exit(self):
        self.log(f"Node '{self.name}' exited")
        #response = self.client.broadcast_message(f"Node '{self.name}' exited")


class ETLTests(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)

    def setUp(self) -> None:
        self.path = f"{systems_tests_path}/{self._testMethodName}"
        self.data_store_path = f"{self.path}/data_store"
        os.makedirs(self.path)
    
    def test_empty_setup(self):
        feature_store_node = FeatureStoreNode(name=f"feature store {self._testMethodName}", path=self.path)
        data_store_node = FileStoreNode(name=f"data store {self._testMethodName}", path=self.data_store_path)
        etl_node = ETLNode(name=f"ETL {self._testMethodName}", data_store=data_store_node, feature_store=feature_store_node)
        pipeline_phase0 = Pipeline(nodes=[data_store_node, etl_node, feature_store_node], logger=logger)
        pipeline_phase0.start()

        self.assertEqual(0, data_store_node.num_predecessors)
        self.assertEqual(1, data_store_node.num_successors)
        
        time.sleep(3)

        for i in range(8):
            data_store_node.save_data_sample(content=f"test {i+1}")
        
        time.sleep(3)

        for i in range(6):
            data_store_node.save_data_sample(content=f"test {i+1}")
        
        time.sleep(3)
        #print("terminating nodes")

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
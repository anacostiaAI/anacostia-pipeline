from typing import Any
import unittest
import logging
import sys
import os
import shutil
import random
from medmnist import PathMNIST, RetinaMNIST
import numpy as np

sys.path.append('..')
sys.path.append('../anacostia_pipeline')
from anacostia_pipeline.resource.data_store import DataStoreNode
from anacostia_pipeline.engine.node import ActionNode, ResourceNode
from anacostia_pipeline.engine.pipeline import Pipeline

from test_utils import *


# Set the seed for reproducibility
seed_value = 42
random.seed(seed_value)

systems_tests_path = "./testing_artifacts/phase2_system_tests"
if os.path.exists(systems_tests_path) is True:
    shutil.rmtree(systems_tests_path)

os.makedirs(systems_tests_path)
os.chmod(systems_tests_path, 0o777)

# Create a logger
log_path = f"{systems_tests_path}/phase2.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='w'
)
logger = logging.getLogger(__name__)


class PrelimDataStoreNode(DataStoreNode):
    def __init__(self, name: str, path: str) -> None:
        self.prelim_path = os.path.join(path, "prelim")
        if os.path.exists(self.prelim_path) is False:
            os.makedirs(self.prelim_path)
        super().__init__(name, self.prelim_path)


class PathMNISTDataStoreNode(DataStoreNode):
    def __init__(self, name: str, split: str, path: str) -> None:
        self.path = os.path.join(path, split)
        if os.path.exists(self.path) is False:
            os.makedirs(self.path)
        
        super().__init__(name, self.path)
        
    def save_data_sample(self, path: str) -> None:
        filename = path.split("/")[-1]
        filename, extension = filename.split(".")

        filedir = os.path.join(self.path, filename)
        os.makedirs(filedir, exist_ok=True)

        new_path = f'{filedir}/pathmnist.npz'
        val_array = np.load(path)
        test_images, test_labels = self.load_test_data()

        np.savez(
            new_path, 
            train_images=val_array, 
            val_images=val_array, 
            test_images=test_images, 
            test_labels=test_labels
        )
        
        self.log(f"Saved data sample: {path} to {new_path}")
        #self.log(f"shape of training array: {val_array.shape}, shape of test array: {test_array.shape}")

    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def load_test_data(self) -> Any:
        test_array = np.load("./testing_artifacts/pathmnist.npz")
        test_images = test_array["test_images"]
        test_labels = test_array["test_labels"]
        return test_images, test_labels


class DataPreprocessingNode(ActionNode):
    def __init__(self, name: str, data_store: DataStoreNode, output_store: DataStoreNode) -> None:
        self.data_store = data_store
        self.output_store = output_store
        super().__init__(name, "preprocess", listen_to=[data_store])
    
    def execute(self) -> bool:
        current_data_filepaths = self.data_store.load_data_paths("current")
        filepath = current_data_filepaths[-1]
        self.log(f"Processing data sample: {filepath}")
        self.output_store.save_data_sample(filepath)
        return True


class RetrainingTests(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)

    def setUp(self) -> None:
        self.path = f"{systems_tests_path}/{self._testMethodName}"
        self.data_path = "./testing_artifacts"
        self.data_store_path = f"{self.path}/data_store"
        os.makedirs(self.path)
    
    def test_initial_setup(self):
        prelim_store = PrelimDataStoreNode(name="Prelim store", path=self.data_store_path)
        medmnist_store = PathMNISTDataStoreNode(name="PathMNIST test store", split="test", path=f"{self.data_store_path}/PathMNIST")
        data_preprocessing = DataPreprocessingNode(name="Data preprocessing", data_store=prelim_store, output_store=medmnist_store)
        pipeline_phase2 = Pipeline(nodes=[prelim_store, data_preprocessing, medmnist_store], logger=logger)
        pipeline_phase2.start()

        time.sleep(1)
        for path in os.listdir("./testing_artifacts/data_store/val_splits"):
            shutil.copy(
                src=os.path.join("./testing_artifacts/data_store/val_splits", path), 
                dst=prelim_store.prelim_path
            )
            time.sleep(0.5)
        time.sleep(1)

        """
        time.sleep(5)
        for i, (img, label) in enumerate(medmnist_store):
            print(i, img, label)
            if i == 5:
                break
        time.sleep(5)
        """

        pipeline_phase2.terminate_nodes()


if __name__ == "__main__":
    unittest.main()
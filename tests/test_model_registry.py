import unittest
import logging
import sys
import os
import shutil
import random

import torch
from torch import nn
from medmnist import INFO

sys.path.append('..')
sys.path.append('../anacostia_pipeline')
from anacostia_pipeline.resource.model_registry import ModelRegistryNode
from anacostia_pipeline.engine.pipeline import Pipeline

from test_utils import *


# Set the seed for reproducibility
seed_value = 42
random.seed(seed_value)

model_registry_tests_path = "./testing_artifacts/model_registry_tests"
if os.path.exists(model_registry_tests_path) is True:
    shutil.rmtree(model_registry_tests_path)

os.makedirs(model_registry_tests_path)
os.chmod(model_registry_tests_path, 0o777)

log_path = f"{model_registry_tests_path}/model_registry.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='w'
)

# Create a logger
logger = logging.getLogger(__name__)


class Net(nn.Module):
    def __init__(self, in_channels, num_classes):
        super(Net, self).__init__()

        self.layer1 = nn.Sequential(
            nn.Conv2d(in_channels, 16, kernel_size=3),
            nn.BatchNorm2d(16),
            nn.ReLU())

        self.layer2 = nn.Sequential(
            nn.Conv2d(16, 16, kernel_size=3),
            nn.BatchNorm2d(16),
            nn.ReLU(),
            nn.MaxPool2d(kernel_size=2, stride=2))

        self.layer3 = nn.Sequential(
            nn.Conv2d(16, 64, kernel_size=3),
            nn.BatchNorm2d(64),
            nn.ReLU())
        
        self.layer4 = nn.Sequential(
            nn.Conv2d(64, 64, kernel_size=3),
            nn.BatchNorm2d(64),
            nn.ReLU())

        self.layer5 = nn.Sequential(
            nn.Conv2d(64, 64, kernel_size=3, padding=1),
            nn.BatchNorm2d(64),
            nn.ReLU(),
            nn.MaxPool2d(kernel_size=2, stride=2))

        self.fc = nn.Sequential(
            nn.Linear(64 * 4 * 4, 128),
            nn.ReLU(),
            nn.Linear(128, 128),
            nn.ReLU(),
            nn.Linear(128, num_classes))

    def forward(self, x):
        x = self.layer1(x)
        x = self.layer2(x)
        x = self.layer3(x)
        x = self.layer4(x)
        x = self.layer5(x)
        x = x.view(x.size(0), -1)
        x = self.fc(x)
        return x


class PyTorchModelRegistryNode(ModelRegistryNode):
    def __init__(self, name: str, path: str) -> None:
        super().__init__(name, path, "pytorch")
    
    def save_model(self, model: torch.nn.Module) -> None:
        model_name = self.create_filename()
        torch.save(model.state_dict(), f"{self.model_registry_path}/{model_name}")
        self.log(f"saved model {model_name} to registry {self.model_registry_path}")
    
    def load_model(self, model: torch.nn.Module) -> torch.nn.Module:
        model_paths = self.get_models_paths("current", return_immediately=False)

        while len(model_paths) == 0:
            model_paths = self.get_models_paths("current")

        model_path = model_paths[0]
        model.load_state_dict(torch.load(model_path))

        self.log(f"loaded model {model_path}")
        return model

class RegistryTests(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)

    def setUp(self) -> None:
        self.path = f"{model_registry_tests_path}/{self._testMethodName}"
        os.makedirs(self.path)

    def test_empty_setup(self):
        model_registry_node = PyTorchModelRegistryNode(name=f"model registry {self._testMethodName}", path=self.path)
        pipeline = Pipeline(nodes=[model_registry_node], logger=logger)
        pipeline.start()

        time.sleep(1)

        data_flag = 'pathmnist'
        info = INFO[data_flag]
        n_channels = info['n_channels']
        n_classes = len(info['label'])
        model = Net(in_channels=n_channels, num_classes=n_classes)
        model_registry_node.save_model(model)

        # note: we need to sleep for a second to allow the observer to pick up the new file 
        # and for the model registry to update its state
        # time.sleep(1)

        loaded_model = model_registry_node.load_model(model)
        print(loaded_model)

        time.sleep(1)

        pipeline.terminate_nodes()


if __name__ == "__main__":
    unittest.main()
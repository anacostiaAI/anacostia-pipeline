from typing import Any, List
import unittest
import logging
import sys
import os
import shutil
import random
from medmnist import PathMNIST, Evaluator, INFO
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
import torch.utils.data as data
import torchvision.transforms as transforms
import pandas as pd
import matplotlib
matplotlib.use('Agg') 
from matplotlib import pyplot as plt
from PIL import Image

sys.path.append('..')
sys.path.append('../anacostia_pipeline')
from anacostia_pipeline.resource.data_store import DataStoreNode
from anacostia_pipeline.resource.metadata_store import MetadataStoreNode
from anacostia_pipeline.resource.model_registry import ModelRegistryNode
from anacostia_pipeline.engine.node import ActionNode, BaseNode
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



# Define constants
data_flag = 'pathmnist'

NUM_EPOCHS = 3
BATCH_SIZE = 128
lr = 0.001

info = INFO[data_flag]
task = info['task']
n_channels = info['n_channels']
n_classes = len(info['label'])

data_transform = transforms.Compose([
    transforms.ToTensor(),
    transforms.Normalize(mean=[.5], std=[.5])
])
            
# define a simple CNN model
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

model = Net(in_channels=n_channels, num_classes=n_classes)



class PrelimDataStoreNode(DataStoreNode):
    def __init__(self, name: str, path: str) -> None:
        self.prelim_path = os.path.join(path, "prelim")
        if os.path.exists(self.prelim_path) is False:
            os.makedirs(self.prelim_path)
        super().__init__(name, self.prelim_path)


class PathMNISTDataStoreNode(DataStoreNode):
    def __init__(self, name: str, path: str) -> None:
        self.path = path
        if os.path.exists(self.path) is False:
            os.makedirs(self.path)
        super().__init__(name, self.path)
        
    def save_data_sample(self, output_path: str, **kwargs) -> None:
        np.savez(output_path, **kwargs)
    
    @DataStoreNode.exeternally_accessible
    @DataStoreNode.resource_accessor
    def load_test_data(self, filepath: str) -> Any:
        test_array = np.load(filepath)
        test_images = test_array["test_images"]
        test_labels = test_array["test_labels"]
        return test_images, test_labels
        

class DataPreprocessingNode(ActionNode):
    def __init__(self, name: str, data_store: DataStoreNode, output_store: DataStoreNode) -> None:
        self.data_store = data_store
        self.output_store = output_store
        super().__init__(name, "preprocess", listen_to=[data_store])
    
    def load_test_data(self) -> Any:
        test_array = np.load("./testing_artifacts/pathmnist.npz")
        test_images = test_array["test_images"]
        test_labels = test_array["test_labels"]
        return test_images, test_labels

    def execute(self) -> bool:
        # Note: this is a hacky way to ensure that the data store does not update its reference count 
        # (and by extension update its state) while we are processing data.
        # This is a temporary solution until we implement a more robust locking mechanism.
        while True:
            with self.data_store.reference_lock:
                self.data_store.reference_count += 1
                break

        for filepath in self.data_store.load_data_paths("current"):

            self.log(f"Processing data sample: {filepath}")
            
            filename = filepath.split("/")[-1]
            filename, extension = filename.split(".")

            filedir = os.path.join(self.output_store.path, filename)
            os.makedirs(filedir, exist_ok=True)

            output_path = f'{filedir}/pathmnist.npz'
            val_array = np.load(filepath)
            val_images = val_array["val_images"]
            val_labels = val_array["val_labels"]
            test_images, test_labels = self.load_test_data()
            
            self.output_store.save_data_sample(
                output_path, 
                train_images=val_images,
                train_labels=val_labels, 
                test_images=test_images, 
                test_labels=test_labels
            )
            self.log(f"Saved data sample: {filepath} to {output_path}")

        while True:
            with self.data_store.reference_lock:
                self.data_store.reference_count -= 1
                if self.data_store.reference_count == 0:
                    self.data_store.event.set()
                break

        return True


class RetrainingNode(ActionNode):
    def __init__(
        self, 
        name: str, 
        data_preprocessing: DataPreprocessingNode, 
        data_store: DataStoreNode, 
        model_registry: ModelRegistryNode,
        metadata_store: MetadataStoreNode,
        artifact_store: DataStoreNode,
    ) -> None:

        self.data_preprocessing = data_preprocessing
        self.data_store = data_store
        self.model_registry = model_registry
        self.metadata_store = metadata_store
        self.artifact_store = artifact_store
        super().__init__(name, "retraining", listen_to=[data_store])
    
    def train_model(self, data_dir: str, model: torch.nn.Module, num_batches: int = None) -> nn.Module:
            # Load data from data store and create data loaders
            train_dataset = PathMNIST(split="train", transform=data_transform, root=data_dir)
            train_loader = data.DataLoader(dataset=train_dataset, batch_size=BATCH_SIZE, shuffle=True)

            # define loss function and optimizer
            if task == "multi-label, binary-class":
                criterion = nn.BCEWithLogitsLoss()
            else:
                criterion = nn.CrossEntropyLoss()
                
            optimizer = optim.SGD(model.parameters(), lr=lr, momentum=0.9)

            progress = 0
            task_duration = len(train_dataset) * NUM_EPOCHS
            for epoch in range(NUM_EPOCHS):
                
                model.train()
                for batch, (inputs, targets) in enumerate(train_loader):
                    if num_batches is not None:
                        if batch >= num_batches:
                            break
            
                    # forward + backward + optimize
                    optimizer.zero_grad()
                    outputs = model(inputs)
                    
                    if task == 'multi-label, binary-class':
                        targets = targets.to(torch.float32)
                        loss = criterion(outputs, targets)
                    else:
                        targets = targets.squeeze().long()
                        loss = criterion(outputs, targets)
                    
                    loss.backward()
                    optimizer.step()

                    progress += len(inputs)
                    
            print(f"Trained {progress} out of {task_duration}")
    
    def validate(self, data_dir: str, model: nn.Module) -> float:
        # load the data from the validation set
        val_dataset = PathMNIST(split="val", transform=data_transform, root=data_dir)
        val_loader = data.DataLoader(dataset=val_dataset, batch_size=BATCH_SIZE, shuffle=True)

        # define loss function and optimizer
        if task == "multi-label, binary-class":
            criterion = nn.BCEWithLogitsLoss()
        else:
            criterion = nn.CrossEntropyLoss()
            
        optimizer = optim.SGD(model.parameters(), lr=lr, momentum=0.9)

        for epoch in range(NUM_EPOCHS):
            train_correct = 0
            train_total = 0
            test_correct = 0
            test_total = 0
            
            model.train()
            for inputs, targets in val_loader:
                # forward + backward + optimize
                optimizer.zero_grad()
                outputs = model(inputs)
                
                if task == 'multi-label, binary-class':
                    targets = targets.to(torch.float32)
                    loss = criterion(outputs, targets)
                else:
                    targets = targets.squeeze().long()
                    loss = criterion(outputs, targets)
                
                loss.backward()
                optimizer.step()

    def evaluate(self, split: str, data_dir: str, model: nn.Module):
        model.eval()
        y_true = torch.tensor([])
        y_score = torch.tensor([])
        
        test_dataset = PathMNIST(split="test", transform=data_transform, root=data_dir)
        train_dataset = PathMNIST(split="train", transform=data_transform, root=data_dir)
        train_loader_at_eval = data.DataLoader(dataset=train_dataset, batch_size=2*BATCH_SIZE, shuffle=False)
        test_loader = data.DataLoader(dataset=test_dataset, batch_size=2*BATCH_SIZE, shuffle=False)

        data_loader = train_loader_at_eval if split == 'train' else test_loader

        with torch.no_grad():
            for inputs, targets in data_loader:
                outputs = model(inputs)

                if task == 'multi-label, binary-class':
                    targets = targets.to(torch.float32)
                    outputs = outputs.softmax(dim=-1)
                else:
                    targets = targets.squeeze().long()
                    outputs = outputs.softmax(dim=-1)
                    targets = targets.float().resize_(len(targets), 1)

                y_true = torch.cat((y_true, targets), 0)
                y_score = torch.cat((y_score, outputs), 0)

            y_true = y_true.numpy()
            y_score = y_score.detach().numpy()
            
            evaluator = Evaluator(data_flag, split, root=data_dir)
            metrics = evaluator.evaluate(y_score)
            print('%s  auc: %.3f  acc:%.3f' % (split, *metrics))

            auc = metrics[0]
            acc = metrics[1]
            self.metadata_store.insert_metadata(split=split, auc=auc, acc=acc)

            # save the montages
            train_dataset.montage(length=5, save_folder=self.artifact_store.data_store_path)
            test_dataset.montage(length=5, save_folder=self.artifact_store.data_store_path)

            # save the metrics as a table
            data_dict = {'split': split, 'auc': auc, 'acc': acc}
            df = pd.DataFrame.from_dict(data_dict, orient='index').reset_index()
            df.columns = ['Attribute', 'Value']
            plt.table(cellText=df.values, colLabels=df.columns, loc='center')
            plt.axis('off')
            plt.savefig(f"{self.artifact_store.data_store_path}/table.jpg", bbox_inches='tight', pad_inches=0)

    def execute(self, *args, **kwargs) -> bool:
        while True:
            with self.metadata_store.reference_lock:
                self.metadata_store.reference_count += 1
                break

        while True:
            with self.model_registry.reference_lock:
                self.model_registry.reference_count += 1
                break

        while True:
            with self.data_store.reference_lock:
                self.data_store.reference_count += 1
                break

        model_paths = self.model_registry.get_models_paths("current")
        model.load_state_dict(torch.load(model_paths[-1]))
        self.log(f"Loaded model from {model_paths[-1]}")
        print(f"Loaded model from {model_paths[-1]}")

        # Save model
        #model_name = self.model_registry.create_filename()
        #torch.save(model.state_dict(), f"{self.model_registry.model_registry_path}/{model_name}")
        #self.log(f"Saved model {model_name} to registry")
        #print(f"Saved model {model_name} to registry")

        for data_file in self.data_store.load_data_paths("current"):
            # Load data
            self.log(f"Retraining model with data file: {data_file}")
            print(f"Retraining model with data file: {data_file}")
        """
            data_dir = data_file.split("/")[:-1]
            data_dir = "/".join(data_dir)
            num_batches = 2
            self.train_model(data_dir, model, num_batches)
        """
        #self.validate("./testing_artifacts", model)

        self.evaluate("train", "./testing_artifacts", model)
        self.evaluate("test", "./testing_artifacts", model)

        self.log(f"Saving report to {self.artifact_store.data_store_path}/report.pdf")
        print(f"Saving report to {self.artifact_store.data_store_path}/report.pdf")

        image_1 = Image.open(f"{self.artifact_store.data_store_path}/pathmnist_train_montage.jpg")
        image_2 = Image.open(f"{self.artifact_store.data_store_path}/pathmnist_test_montage.jpg")
        image_3 = Image.open(f"{self.artifact_store.data_store_path}/table.jpg")
        
        im_1 = image_1.convert('RGB')
        im_2 = image_2.convert('RGB')
        im_3 = image_3.convert('RGB')

        image_list = [im_2, im_3]

        im_1.save(f'{self.artifact_store.data_store_path}/report.pdf', save_all=True, append_images=image_list)

        while True:
            with self.data_store.reference_lock:
                self.data_store.reference_count -= 1
                if self.data_store.reference_count == 0:
                    self.data_store.event.set()
                break
        
        while True:
            with self.model_registry.reference_lock:
                self.model_registry.reference_count -= 1
                if self.model_registry.reference_count == 0:
                    self.model_registry.event.set()
                break
                
        while True:
            with self.metadata_store.reference_lock:
                self.metadata_store.reference_count -= 1
                if self.metadata_store.reference_count == 0:
                    self.metadata_store.event.set()
                break
                
        return True


class ReportingNode(ActionNode):
    def __init__(self, name: str, train_node: RetrainingNode, artifact_store: DataStoreNode) -> None:
        self.artifact_store = artifact_store
        #super().__init__(name, "Reporting", listen_to=[train_node])
        super().__init__(name, "Reporting", listen_to=[artifact_store])
    
    def execute(self, *args, **kwargs) -> bool:

        self.log(f"Saving report to {self.artifact_store.data_store_path}/report.pdf")
        print(f"Saving report to {self.artifact_store.data_store_path}/report.pdf")

        image_1 = Image.open(f"{self.artifact_store.data_store_path}/pathmnist_train_montage.png")
        image_2 = Image.open(f"{self.artifact_store.data_store_path}/pathmnist_test_montage.png")
        
        im_1 = image_1.convert('RGB')
        im_2 = image_2.convert('RGB')

        image_list = [im_2]

        im_1.save(f'{self.artifact_store.data_store_path}/report.pdf', save_all=True, append_images=image_list)

        return True

class RetrainingTests(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)

    def setUp(self) -> None:
        self.path = f"{systems_tests_path}/{self._testMethodName}"
        self.data_path = "./testing_artifacts"
        self.data_store_path = f"{self.path}/data_store"
        self.meta_data_store_path = f"{self.path}/metadata_store"
        os.makedirs(self.path)
    
    def test_initial_setup(self):
        prelim_store = PrelimDataStoreNode(name="Prelim store", path=self.data_store_path)
        medmnist_store = PathMNISTDataStoreNode(name="PathMNIST store", path=f"{self.data_store_path}/PathMNIST")
        data_preprocessing = DataPreprocessingNode(name="Data preprocessing", data_store=prelim_store, output_store=medmnist_store)
        metadata_store = MetadataStoreNode(name="Metadata store", metadata_store_path=self.meta_data_store_path, init_state="current")
        artifact_store = DataStoreNode(name="Artifact store", path=f"{self.path}/artifact_store", init_state="current")
        model_registry = ModelRegistryNode(
            name="Model registry", 
            path=self.path, 
            framework="pytorch", 
            init_state="current"
        )
        retraining = RetrainingNode(
            name="Retraining", 
            data_preprocessing=data_preprocessing, 
            data_store=medmnist_store, 
            model_registry=model_registry, 
            metadata_store=metadata_store,
            artifact_store=artifact_store
        )
        reporting = ReportingNode(name="Reporting", train_node=retraining, artifact_store=artifact_store)
        pipeline_phase2 = Pipeline(
            nodes=[prelim_store, data_preprocessing, medmnist_store, retraining, model_registry, metadata_store, artifact_store], 
            #nodes=[reporting, artifact_store],
            logger=logger
        )
        pipeline_phase2.start()

        time.sleep(1)

        # copy files to model registry
        shutil.copy(
            src="./testing_artifacts/model.pth", 
            dst=model_registry.model_registry_path
        )

        """
        shutil.copy(
            src="./testing_artifacts/pathmnist_train_montage.png", 
            dst=artifact_store.data_store_path
        )

        shutil.copy(
            src="./testing_artifacts/pathmnist_test_montage.png", 
            dst=artifact_store.data_store_path
        )
        """

        # copy files to prelim store
        files_list = [
            "./testing_artifacts/data_store/val_splits/val_4.npz", 
            "./testing_artifacts/data_store/val_splits/val_5.npz", 
            "./testing_artifacts/data_store/val_splits/val_6.npz", 
            "./testing_artifacts/data_store/val_splits/val_7.npz", 
        ]
        for path in files_list:
            shutil.copy(
                src=path, 
                dst=prelim_store.prelim_path
            )
            time.sleep(1)
        
        # Wait for training to finish
        time.sleep(30)
        #time.sleep(2)
        #time.sleep(240)

        pipeline_phase2.terminate_nodes()


if __name__ == "__main__":
    unittest.main()
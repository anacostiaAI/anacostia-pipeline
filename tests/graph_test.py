from logging import Logger
import os
import time
import logging
import shutil
from typing import List
import json
import requests
import time
from dotenv import load_dotenv

from anacostia_pipeline.engine.base import BaseNode, BaseActionNode, BaseMetadataStoreNode
from anacostia_pipeline.engine.pipeline import Pipeline
from anacostia_pipeline.dashboard.webserver import run_background_webserver

from anacostia_pipeline.resources.filesystem_store import FilesystemStoreNode
from anacostia_pipeline.metadata.sql_metadata_store import SqliteMetadataStore

from utils import *

# Make sure that the .env file is in the same directory as this Python script
load_dotenv()


class MetadataStore(SqliteMetadataStore):
    def __init__(
        self, name: str, uri: str, temp_dir: str, loggers: Logger | List[Logger] = None
    ) -> None:
        super().__init__(name, uri, loggers)
        self.temp_dir = temp_dir
        os.makedirs(self.temp_dir)
    
    def get_runs_json(self, path: str):
        runs = self.get_runs()
        runs = [run.as_dict() for run in runs]
        
        for run in runs:
            run["start_time"] = str(run["start_time"])
            if run["end_time"] != None:
                run["end_time"] = str(run["end_time"])
        
        with open(path, "w") as f:
            json.dump(runs, f, ensure_ascii=False, indent=4) 
    
    def get_metrics_json(self, path: str):
        metrics = self.get_metrics()
        with open(path, "w") as f:
            json.dump(metrics, f, ensure_ascii=False, indent=4)  
    
    def get_params_json(self, path: str):
        params = self.get_params()
        with open(path, "w") as f:
            json.dump(params, f, ensure_ascii=False, indent=4)  
    
    def get_tags_json(self, path: str):
        tags = self.get_tags()
        with open(path, "w") as f:
            json.dump(tags, f, ensure_ascii=False, indent=4)  


class MonitoringDataStoreNode(FilesystemStoreNode):
    def __init__(
        self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, 
        init_state: str = "new", max_old_samples: int = None
    ) -> None:
        super().__init__(name, resource_path, metadata_store, init_state, max_old_samples)
    
    def trigger_condition(self) -> bool:
        num_new = self.get_num_artifacts("new")
        return num_new >= 1


class ModelRegistryNode(FilesystemStoreNode):
    def __init__(self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, ) -> None:
        super().__init__(name, resource_path, metadata_store, init_state="new", max_old_samples=None, monitoring=False)
    
    def create_filename(self) -> str:
        return f"processed_data_file{self.get_num_artifacts('all')}.txt"

    def save_artifact(self, content: str) -> None:
        filename = self.create_filename()
        filepath = os.path.join(self.path, filename)

        # note: for monitoring-enabled resource nodes, record_artifact should be called before create_file;
        # that way, the Observer can see the file is already logged and ignore it
        self.record_current(filepath)
        with open(filepath, 'w') as f:
            f.write(content)
        self.log(f"Saved preprocessed {filepath}", level="INFO")


class PlotsStoreNode(FilesystemStoreNode):
    def __init__(self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, ) -> None:
        super().__init__(name, resource_path, metadata_store, init_state="new", max_old_samples=None, monitoring=False)
    

class ModelRetrainingNode(BaseActionNode):
    def __init__(
        self, name: str, 
        data_store: MonitoringDataStoreNode, plots_store: PlotsStoreNode,
        model_registry: ModelRegistryNode, metadata_store: BaseMetadataStoreNode
    ) -> None:
        self.data_store = data_store
        self.model_registry = model_registry
        self.plots_store = plots_store
        self.metadata_store = metadata_store
        super().__init__(name, predecessors=[data_store, plots_store, model_registry])
    
    def execute(self, *args, **kwargs) -> bool:
        self.log(f"Executing node '{self.name}'", level="INFO")

        for filepath in self.data_store.list_artifacts("current"):
            with open(filepath, 'r') as f:
                self.log(f"Trained on {filepath}", level="INFO")
        
        for filepath in self.data_store.list_artifacts("old"):
            self.log(f"Already trained on {filepath}", level="INFO")
        
        self.metadata_store.log_metrics(acc=1.00)
        
        self.metadata_store.log_params(
            batch_size = 64, # how many independent sequences will we process in parallel?
            block_size = 256, # what is the maximum context length for predictions?
            max_iters = 2500,
            eval_interval = 500,
            learning_rate = 3e-4,
            eval_iters = 200,
            n_embd = 384,
            n_head = 6,
            n_layer = 6,
            dropout = 0.2,
            seed = 1337,
            split = 0.9    # first 90% will be train, rest val
        )

        self.metadata_store.set_tags(test_name="Karpathy LLM test")

        self.log(f"Node '{self.name}' executed successfully.", level="INFO")
        return True


class ShakespeareEvalNode(BaseActionNode):
    def __init__(
        self, name: str, predecessors: List[BaseNode], 
        metadata_store: BaseMetadataStoreNode, loggers: Logger | List[Logger] = None
    ) -> None:
        self.metadata_store = metadata_store
        super().__init__(name, predecessors, loggers)
    
    def execute(self, *args, **kwargs) -> bool:
        self.log("Evaluating LLM on Shakespeare validation dataset", level="INFO")
        self.metadata_store.log_metrics(shakespeare_test_loss=1.47)
        return True

class HaikuEvalNode(BaseActionNode):
    def __init__(
        self, name: str, predecessors: List[BaseNode], 
        metadata_store: BaseMetadataStoreNode, loggers: Logger | List[Logger] = None
    ) -> None:
        self.metadata_store = metadata_store
        super().__init__(name, predecessors, loggers)
    
    def execute(self, *args, **kwargs) -> bool:
        self.log("Evaluating LLM on Haiku validation dataset", level="INFO")
        self.metadata_store.log_metrics(haiku_test_loss=2.43)
        return True

class IpfsUpload(BaseActionNode):
    def __init__(
        self, name: str, predecessors: List[BaseNode], metadata_store: MetadataStore, loggers: Logger | List[Logger] = None
    ) -> None: 
        super().__init__(name, predecessors, loggers)
        self.metadata_store = metadata_store
        self.temp_dir = self.metadata_store.temp_dir
        self.api_key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkaWQ6ZXRocjoweDQxNzNjMUI0ODczZGM5RGY0NkVFZjQ1ZWQ1ZTIxZDliMzFjNTY0RUYiLCJpc3MiOiJuZnQtc3RvcmFnZSIsImlhdCI6MTcwNjQ3MDI3ODIxMCwibmFtZSI6IldpbnRlcnNHYXJkZW5Db2xsZWN0aW9uIn0.bAhskZsIg3i0-1LT-OJ9nsxtIaWY_3VmKqlNE9BvsOs'  # Replace with your actual NFT.Storage API key

    def execute(self, *args, **kwargs) -> bool:
        url = 'https://api.nft.storage/upload'

        self.metadata_store.get_runs_json(path=f"{self.metadata_store.temp_dir}/filename1.json")

        time.sleep(1)

        # Open the file in binary mode
        with open(f"{self.temp_dir}/filename1.json", 'rb') as file:
            headers = {
                'Authorization': f'Bearer {self.api_key}',
                'Content-Type': 'application/octet-stream'
            }
            # Make the POST request to upload the file within the 'with' block
            response = requests.post(url, headers=headers, data=file)

        # The rest of the code remains outside of the 'with' block
        print("running")
        if response.status_code == 200:
            ipfs_hash = response.json()['value']['cid']
            print(f'File uploaded to NFT.Storage with hash: {ipfs_hash}')
            return True  # Return True if the upload was successful
        else:
            print(f'Error uploading file: {response.text}')
            return False
            

    


web_tests_path = "./testing_artifacts/frontend"
if os.path.exists(web_tests_path) is True:
    shutil.rmtree(web_tests_path)
os.makedirs(web_tests_path)

log_path = f"{web_tests_path}/anacostia.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='a'
)
logger = logging.getLogger(__name__)


path = f"{web_tests_path}/webserver_test"
metadata_store_path = f"{path}/metadata_store"
haiku_data_store_path = f"{path}/haiku"
model_registry_path = f"{path}/model_registry"
plots_path = f"{path}/plots"
json_dir = f'{path}/ipfsjson'  # Adjust the file path if necessary

metadata_store = MetadataStore(
    name="metadata_store", 
    uri=f"sqlite:///{metadata_store_path}/metadata.db",
    temp_dir=json_dir
)
model_registry = ModelRegistryNode(
    "model_registry", 
    model_registry_path, 
    metadata_store
)
plots_store = PlotsStoreNode("plots_store", plots_path, metadata_store)
haiku_data_store = MonitoringDataStoreNode("haiku_data_store", haiku_data_store_path, metadata_store)
retraining = ModelRetrainingNode("retraining", haiku_data_store, plots_store, model_registry, metadata_store)
shakespeare_eval = ShakespeareEvalNode("shakespeare_eval", predecessors=[retraining], metadata_store=metadata_store)
haiku_eval = HaikuEvalNode("haiku_eval", predecessors=[retraining], metadata_store=metadata_store)
IpfsUploadnode = IpfsUpload("Ipfsnode", predecessors=[shakespeare_eval,haiku_eval], metadata_store=metadata_store)
pipeline = Pipeline(
    nodes=[metadata_store, haiku_data_store, model_registry, plots_store, shakespeare_eval, haiku_eval, retraining,IpfsUploadnode], 
    loggers=logger
)



if __name__ == "__main__":
    run_background_webserver(pipeline, host="127.0.0.1", port=8000)

    time.sleep(6)
    for i in range(10):
        create_file(f"{haiku_data_store_path}/test_file{i}.txt", f"test file {i}")
        time.sleep(1.5)

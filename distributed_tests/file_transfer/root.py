import logging
import argparse
from typing import List
from pathlib import Path
from logging.config import dictConfig

from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.resources.filesystem.utils import locked_file
from anacostia_pipeline.nodes.metadata.sql.sqlite.node import SQLiteMetadataStoreNode
from anacostia_pipeline.pipelines.pipeline import Pipeline
from anacostia_pipeline.pipelines.server import PipelineServer, AnacostiaServer

from utils import *
from loggers import ROOT_ACCESS_LOGGING_CONFIG, ROOT_ANACOSTIA_LOGGING_CONFIG



parser = argparse.ArgumentParser()
parser.add_argument('root_host', type=str)
parser.add_argument('root_port', type=int)
parser.add_argument('leaf_host', type=str)
parser.add_argument('leaf_port', type=int)
args = parser.parse_args()

dictConfig(ROOT_ANACOSTIA_LOGGING_CONFIG)
logger = logging.getLogger("root_anacostia")

mkcert_ca = Path(os.popen("mkcert -CAROOT").read().strip()) / "rootCA.pem"
mkcert_ca = str(mkcert_ca)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ssl_certfile = os.path.join(BASE_DIR, "certs/certificate_leaf.pem")
ssl_keyfile = os.path.join(BASE_DIR, "certs/private_leaf.key")



def save_model(filepath: str, content: str) -> None:
    with locked_file(filepath, 'w') as f:
        f.write(content)

def load_model(filepath: str) -> None:
    with locked_file(filepath, "r") as file:
        return file.read()


class MonitoringDataStoreNode(FilesystemStoreNode):
    def __init__(self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, max_old_samples: int = None) -> None:
        super().__init__(name=name, resource_path=resource_path, metadata_store=metadata_store, max_old_samples=max_old_samples)

    def load_artifact(self, filepath: str, *args, **kwargs):
        with super().load_artifact(filepath) as fullpath:
            return load_model(fullpath)


class ModelRegistryNode(FilesystemStoreNode):
    def __init__(self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, client_url: str) -> None:
        super().__init__(name, resource_path, metadata_store, max_old_samples=None, client_url=client_url, monitoring=False)

    def save_artifact(self, filepath: str, content: str, *args, **kwargs):
        with super().save_artifact(filepath) as fullpath:
            save_model(fullpath, content)

    def load_artifact(self, filepath: str, *args, **kwargs):
        with super().load_artifact(filepath) as fullpath:
            return load_model(fullpath)
    

class PlotsStoreNode(FilesystemStoreNode):
    def __init__(self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, client_url: str) -> None:
        super().__init__(name, resource_path, metadata_store, max_old_samples=None, client_url=client_url, monitoring=False)
    
    def load_artifact(self, filepath: str, *args, **kwargs):
        with super().load_artifact(filepath) as fullpath:
            return load_model(fullpath)
    

class ModelRetrainingNode(BaseActionNode):
    def __init__(
        self, name: str, 
        data_store: MonitoringDataStoreNode, plots_store: PlotsStoreNode,
        model_registry: ModelRegistryNode, metadata_store: BaseMetadataStoreNode, 
        remote_successors: List[str] = None
    ) -> None:
        self.data_store = data_store
        self.model_registry = model_registry
        self.plots_store = plots_store
        self.metadata_store = metadata_store
        super().__init__(name, predecessors=[data_store, plots_store, model_registry], remote_successors=remote_successors)
    
    def execute(self, *args, **kwargs) -> bool:
        self.log(f"Executing node '{self.name}'", level="INFO")

        current_artifacts = self.data_store.list_artifacts("new")
        for filepath in current_artifacts:
            content = self.data_store.load_artifact(filepath)
            self.log(f"current {filepath} content: {content}", level="INFO")

        # Simulate saving a trained model
        num_artifacts = self.model_registry.get_num_artifacts('all')

        self.model_registry.save_artifact(
            filepath=f"model{num_artifacts}.txt", content="Trained model"
        )
        self.metadata_store.tag_artifact(
            self.name, location=f"model{num_artifacts}.txt", model_type="LLM"
        )

        self.log(f"Node '{self.name}' executed successfully.", level="INFO")
        return True

path = f"./root-artifacts"
input_path = f"{path}/input_artifacts"
output_path = f"{path}/output_artifacts"
metadata_store_path = f"{input_path}/metadata_store"
haiku_data_store_path = f"{input_path}/haiku"
model_registry_path = f"{output_path}/model_registry"
plots_path = f"{output_path}/plots"

metadata_store = SQLiteMetadataStoreNode(
    name="metadata_store", 
    uri=f"sqlite:///{metadata_store_path}/metadata.db",
    client_url=f"https://{args.leaf_host}:{args.leaf_port}/metadata_store_rpc"
)
model_registry = ModelRegistryNode(
    name="model_registry", 
    resource_path=model_registry_path, 
    metadata_store=metadata_store,
    client_url=f"https://{args.leaf_host}:{args.leaf_port}/model_registry_rpc"
)
plots_store = PlotsStoreNode(
    name="plots_store", 
    resource_path=plots_path, 
    metadata_store=metadata_store,
    client_url=f"https://{args.leaf_host}:{args.leaf_port}/plots_store_rpc"
)
haiku_data_store = MonitoringDataStoreNode("haiku_data_store", haiku_data_store_path, metadata_store)
retraining = ModelRetrainingNode(
    name="retraining", 
    data_store=haiku_data_store, 
    plots_store=plots_store, 
    model_registry=model_registry, 
    metadata_store=metadata_store, 
    remote_successors=[f"https://{args.leaf_host}:{args.leaf_port}/shakespeare_eval", f"https://{args.leaf_host}:{args.leaf_port}/haiku_eval"]
)

pipeline = Pipeline(
    name="root_pipeline", 
    nodes=[metadata_store, haiku_data_store, model_registry, plots_store, retraining], 
    loggers=logger
)

service = PipelineServer(
    name="root", 
    pipeline=pipeline, 
    host=args.root_host, 
    port=args.root_port, 
    logger=logger, 
    allow_origins=["https://127.0.0.1:8000", "https://localhost:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    ssl_ca_certs=mkcert_ca,
    ssl_certfile=ssl_certfile,
    ssl_keyfile=ssl_keyfile,
    uvicorn_access_log_config=ROOT_ACCESS_LOGGING_CONFIG
)

config = service.get_config()
server = AnacostiaServer(config=config)

with server.run_in_thread():
    while True:
        try:
            pass    # Keep the server running
        except (KeyboardInterrupt, SystemExit):
            print("Shutting down the server...")
            break

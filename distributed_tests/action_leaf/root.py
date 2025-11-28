import logging
from typing import List
from pathlib import Path
from logging.config import dictConfig

from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.resources.filesystem.utils import locked_file
from anacostia_pipeline.nodes.metadata.sql.sqlite.node import SQLiteMetadataStoreNode
from anacostia_pipeline.pipelines.pipeline import Pipeline
from anacostia_pipeline.pipelines.server import PipelineServer

from utils import *
from loggers import ROOT_ACCESS_LOGGING_CONFIG, ROOT_ANACOSTIA_LOGGING_CONFIG



root_host = "127.0.0.1"
leaf_host = "127.0.0.1"
root_port = 8000
leaf_port = 8001

dictConfig(ROOT_ANACOSTIA_LOGGING_CONFIG)
logger = logging.getLogger("root_anacostia")

mkcert_ca = Path(os.popen("mkcert -CAROOT").read().strip()) / "rootCA.pem"
mkcert_ca = str(mkcert_ca)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ssl_certfile = os.path.join(BASE_DIR, "certs/certificate_leaf.pem")
ssl_keyfile = os.path.join(BASE_DIR, "certs/private_leaf.key")



def save_text_file(filepath: str, content: str) -> None:
    with locked_file(filepath, 'w') as f:
        f.write(content)

def load_text_file(filepath: str) -> None:
    with locked_file(filepath, "r") as file:
        return file.read()


class MonitoringDataStoreNode(FilesystemStoreNode):
    def __init__(
        self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, max_old_samples: int = None
    ) -> None:
        super().__init__(name=name, resource_path=resource_path, metadata_store=metadata_store, max_old_samples=max_old_samples)

    # one way to customize loading behavior is to create an alias that points to load_artifact
    def load_data(self, filepath: str, *args, **kwargs):
        return super().load_artifact(filepath)


class ModelRegistryNode(FilesystemStoreNode):
    def __init__(self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, client_url: str) -> None:
        super().__init__(name, resource_path, metadata_store, max_old_samples=None, client_url=client_url, monitoring=False)

    # another way to customize saving/loading behavior is to create a custom method that wraps the 
    # save_artifact/load_artifact context managers
    def save_model(self, filepath: str, content: str, *args, **kwargs):
        with super().save_artifact(filepath) as fullpath:
            save_text_file(fullpath, content)

    def load_model(self, filepath: str, *args, **kwargs):
        with super().load_artifact(filepath) as fullpath:
            return load_text_file(fullpath)


class PlotsStoreNode(FilesystemStoreNode):
    def __init__(self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, client_url: str) -> None:
        super().__init__(name, resource_path, metadata_store, max_old_samples=None, client_url=client_url, monitoring=False)
    
    def load_plot(self, filepath: str, *args, **kwargs):
        with super().load_artifact(filepath) as fullpath:
            return load_text_file(fullpath)


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
            with self.data_store.load_data(filepath) as fullpath:
                training_data = load_text_file(fullpath)
                self.log(f"Training on {training_data}", level="INFO")
        
                # Simulate saving a trained model
                num_artifacts = self.model_registry.get_num_artifacts('all')

                self.model_registry.save_model(
                    filepath=f"model{num_artifacts}.txt", content="Trained model"
                )
                """
                self.metadata_store.tag_artifact(
                    self.name, location=f"model{num_artifacts}.txt", model_type="LLM"
                )
                """

                # must pass self.name to log_metrics and log_params in order for the metadata store to know which node is logging;
                # otherwise, the metadata store will not know which node is logging and will not enter the data into the database
                self.metadata_store.log_metrics(self.name, acc=1.00)
                
                self.metadata_store.log_params(
                    self.name,
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

                self.metadata_store.set_tags(self.name, test_name="Karpathy LLM test")

        old_artifacts = self.data_store.list_artifacts("used")
        for filepath in old_artifacts:
            with self.data_store.load_data(filepath) as fullpath:
                used_data = load_text_file(fullpath)
                self.log(f"Already trained on {used_data}", level="INFO")

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
    client_url=f"https://{leaf_host}:{leaf_port}/metadata_store_rpc"
)
model_registry = ModelRegistryNode(
    name="model_registry", 
    resource_path=model_registry_path, 
    metadata_store=metadata_store,
    client_url=f"https://{leaf_host}:{leaf_port}/model_registry_rpc"
)
plots_store = PlotsStoreNode(
    name="plots_store", 
    resource_path=plots_path, 
    metadata_store=metadata_store,
    client_url=f"https://{leaf_host}:{leaf_port}/plots_store_rpc"
)
haiku_data_store = MonitoringDataStoreNode("haiku_data_store", haiku_data_store_path, metadata_store)
retraining = ModelRetrainingNode(
    name="retraining", 
    data_store=haiku_data_store, 
    plots_store=plots_store, 
    model_registry=model_registry, 
    metadata_store=metadata_store, 
    remote_successors=[f"https://{leaf_host}:{leaf_port}/shakespeare_eval", f"https://{leaf_host}:{leaf_port}/haiku_eval"]
)

pipeline = Pipeline(
    name="root_pipeline", 
    nodes=[metadata_store, haiku_data_store, model_registry, plots_store, retraining], 
    loggers=logger
)

service = PipelineServer(
    name="root", 
    pipeline=pipeline, 
    host=root_host, 
    port=root_port, 
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

"""
from anacostia_pipeline.pipelines.server import AnacostiaServer

config = service.get_config()
server = AnacostiaServer(config=config)

with server.run_in_thread():
    while True:
        try:
            pass    # Keep the server running
        except (KeyboardInterrupt, SystemExit):
            print("Shutting down the server...")
            break
"""

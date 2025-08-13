import argparse
import logging
from logging import Logger
from typing import List
import os
from pathlib import Path
from logging.config import dictConfig

from anacostia_pipeline.pipelines.pipeline import Pipeline
from anacostia_pipeline.pipelines.server import PipelineServer, AnacostiaServer
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.metadata.sql.api import SQLMetadataStoreClient
from anacostia_pipeline.nodes.resources.filesystem.api import FilesystemStoreClient
from utils import create_file
from loggers import LEAF_ACCESS_LOGGING_CONFIG, LEAF_ANACOSTIA_LOGGING_CONFIG



parser = argparse.ArgumentParser()
parser.add_argument('host', type=str)
parser.add_argument('port', type=int)
args = parser.parse_args()

leaf_test_path = "./testing_artifacts"
path = f"./leaf-artifacts"
input_path = f"{path}/input_artifacts"
output_path = f"{path}/output_artifacts"
model_registry_path = f"{input_path}/model_registry"
plots_path = f"{output_path}/plots"

dictConfig(LEAF_ANACOSTIA_LOGGING_CONFIG)
logger = logging.getLogger("leaf_anacostia")

mkcert_ca = Path(os.popen("mkcert -CAROOT").read().strip()) / "rootCA.pem"
mkcert_ca = str(mkcert_ca)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ssl_certfile = os.path.join(BASE_DIR, "certs/certificate_leaf.pem")
ssl_keyfile = os.path.join(BASE_DIR, "certs/private_leaf.key")



class ShakespeareEvalNode(BaseActionNode):
    def __init__(
        self, name: str, metadata_store_rpc: SQLMetadataStoreClient, model_registry_rpc: FilesystemStoreClient = None,
        loggers: Logger | List[Logger] = None
    ) -> None:
        super().__init__(name=name, predecessors=[], wait_for_connection=True, loggers=loggers)
        self.metadata_store_rpc = metadata_store_rpc
        self.model_registry_rpc = model_registry_rpc
    
    def execute(self, *args, **kwargs) -> bool:
        self.log("Evaluating LLM on Shakespeare validation dataset", level="INFO")
        
        run_id = self.metadata_store_rpc.get_run_id()

        try:
            self.model_registry_rpc.download_artifact(filepath=f"model{run_id}.txt")
        except Exception as e:
            self.log(f"Error downloading artifact: {str(e)}", level="ERROR")
        
        num_artifacts = self.model_registry_rpc.get_num_artifacts()
        artifacts = self.model_registry_rpc.list_artifacts()
        self.log(f"{num_artifacts} Artifacts: {artifacts}", level="INFO")
            
        return True

class HaikuEvalNode(BaseActionNode):
    def __init__(
        self, name: str, metadata_store_rpc: SQLMetadataStoreClient, plots_store_rpc: FilesystemStoreClient = None,
        loggers: Logger | List[Logger] = None
    ) -> None:
        super().__init__(name=name, predecessors=[], wait_for_connection=True, loggers=loggers)
        self.metadata_store_rpc = metadata_store_rpc
        self.plots_store_rpc = plots_store_rpc
    
    def execute(self, *args, **kwargs) -> bool:
        self.log("Evaluating LLM on Haiku validation dataset", level="INFO")
        
        run_id = self.metadata_store_rpc.get_run_id()
        create_file(f"{self.plots_store_rpc.storage_directory}/plot{run_id}.txt", "Haiku test loss plot")

        try:
            self.plots_store_rpc.upload_artifact(filepath=f"plot{run_id}.txt", remote_path=f"plot{run_id}.txt")
        except Exception as e:
            self.log(f"Error uploading artifact: {str(e)}", level="ERROR")

        return True

metadata_store_rpc = SQLMetadataStoreClient(client_name="metadata_store_rpc")
model_registry_rpc = FilesystemStoreClient(storage_directory=model_registry_path, client_name="model_registry_rpc")
plots_store_rpc = FilesystemStoreClient(storage_directory=plots_path, client_name="plots_store_rpc")
shakespeare_eval = ShakespeareEvalNode("shakespeare_eval", metadata_store_rpc=metadata_store_rpc, model_registry_rpc=model_registry_rpc)
haiku_eval = HaikuEvalNode("haiku_eval", metadata_store_rpc=metadata_store_rpc, plots_store_rpc=plots_store_rpc)

pipeline = Pipeline(
    name="leaf_pipeline",
    nodes=[shakespeare_eval, haiku_eval],
    loggers=logger
)

service = PipelineServer(
    name="leaf", 
    pipeline=pipeline, 
    host=args.host, 
    port=args.port, 
    remote_clients=[metadata_store_rpc, model_registry_rpc, plots_store_rpc], 
    logger=logger,
    allow_origins=["https://127.0.0.1:8000", "https://localhost:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    ssl_ca_certs=mkcert_ca,
    ssl_certfile=ssl_certfile,
    ssl_keyfile=ssl_keyfile,
    uvicorn_access_log_config=LEAF_ACCESS_LOGGING_CONFIG
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


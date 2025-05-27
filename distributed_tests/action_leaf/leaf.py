import argparse
import logging
from logging import Logger
from typing import List

from anacostia_pipeline.pipelines.pipeline import Pipeline
from anacostia_pipeline.pipelines.server import PipelineServer
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.metadata.sql.api import SQLMetadataStoreClient
from anacostia_pipeline.nodes.resources.filesystem.api import FilesystemStoreClient
from utils import create_file



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

# Create a file for Anacostia logs
log_path = f"{leaf_test_path}/anacostia.log"
log_file_handler = logging.FileHandler(log_path)
log_file_handler.setLevel(logging.INFO)
log_formatter = logging.Formatter(
    fmt='LEAF %(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log_file_handler.setFormatter(log_formatter)
logger = logging.getLogger(__name__)
logger.addHandler(log_file_handler)
logger.setLevel(logging.INFO)  # Make sure it's at least INFO

# Step 1: Create a file handler for access logs
access_log_path = f"{leaf_test_path}/access.log"

# Uvicorn needs a dictionary describing the log setup
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "fmt": "LEAF ACCESS %(asctime)s - %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "access_file_handler": {
            "level": "INFO",
            "class": "logging.FileHandler",
            "formatter": "default",
            "filename": f"{access_log_path}",   # access_log_path = "./testing_artifacts/access.log" , log_path = "./testing_artifacts/anacostia.log"
        },
    },
    "loggers": {
        "uvicorn.access": {
            "handlers": ["access_file_handler"],
            "level": "INFO",
            "propagate": False,
        },
    },
}


class ShakespeareEvalNode(BaseActionNode):
    def __init__(
        self, name: str, metadata_store_rpc: SQLMetadataStoreClient, model_registry_rpc: FilesystemStoreClient = None,
        loggers: Logger | List[Logger] = None
    ) -> None:
        super().__init__(name=name, predecessors=[], wait_for_connection=True, loggers=loggers)
        self.metadata_store_rpc = metadata_store_rpc
        self.model_registry_rpc = model_registry_rpc
    
    async def execute(self, *args, **kwargs) -> bool:
        self.log("Evaluating LLM on Shakespeare validation dataset", level="INFO")
        
        try:
            await self.metadata_store_rpc.log_metrics(node_name=self.name, shakespeare_test_loss=1.47)
        except Exception as e:
            self.log(f"Failed to log metrics: {e}", level="ERROR")
        
        try:
            run_id = await self.metadata_store_rpc.get_run_id()
            await self.model_registry_rpc.get_artifact(filepath=f"model{run_id}.txt")
        except Exception as e:
            self.log(f"Failed to get artifact: {e}", level="ERROR")
        
        try:
            num_artifacts = await self.model_registry_rpc.get_num_artifacts()
            artifacts = await self.model_registry_rpc.list_artifacts()
            self.log(f"{num_artifacts} Artifacts: {artifacts}", level="INFO")
        except Exception as e:
            self.log(f"Failed to list artifacts: {e}", level="ERROR")
            
        return True

class HaikuEvalNode(BaseActionNode):
    def __init__(
        self, name: str, metadata_store_rpc: SQLMetadataStoreClient, plots_store_rpc: FilesystemStoreClient = None,
        loggers: Logger | List[Logger] = None
    ) -> None:
        super().__init__(name=name, predecessors=[], wait_for_connection=True, loggers=loggers)
        self.metadata_store_rpc = metadata_store_rpc
        self.plots_store_rpc = plots_store_rpc
    
    async def execute(self, *args, **kwargs) -> bool:
        self.log("Evaluating LLM on Haiku validation dataset", level="INFO")

        try:
            await self.metadata_store_rpc.log_metrics(node_name=self.name, haiku_test_loss=2.47)
        except Exception as e:
            self.log(f"Failed to log metrics: {e}", level="ERROR")
        
        try:
            tags = await self.metadata_store_rpc.get_metrics()
            self.log(f"Tags: {tags}", level="INFO")
        except Exception as e:
            self.log(f"Failed to get tags: {e}", level="ERROR")
        
        try:
            run_id = await self.metadata_store_rpc.get_run_id()

            create_file(f"{self.plots_store_rpc.storage_directory}/plot{run_id}.txt", "Haiku test loss plot")
            
            await self.plots_store_rpc.upload_file(
                filepath=f"plot{run_id}.txt",
                remote_path=f"plot{run_id}.txt"
            )
        except Exception as e:
            self.log(f"Failed to create plot: {e}", level="ERROR")

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
    uvicorn_access_log_config=LOGGING_CONFIG
)
service.run()

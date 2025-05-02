from typing import List, Dict, Union
import logging
from logging import Logger
import argparse

from fastapi.middleware.cors import CORSMiddleware
from anacostia_pipeline.pipelines.leaf.server import LeafPipelineServer
from anacostia_pipeline.pipelines.leaf.pipeline import LeafPipeline
from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.metadata.sql.rpc import SQLMetadataRPCCaller


parser = argparse.ArgumentParser()
parser.add_argument('host', type=str)
parser.add_argument('port', type=int)
args = parser.parse_args()

leaf_test_path = "./testing_artifacts"
path = f"./leaf-artifacts"
input_path = f"{path}/input_artifacts"
output_path = f"{path}/output_artifacts"
shakespeare_input_path = f"{input_path}/shakespeare"
shakespeare_output_path = f"{output_path}/shakespeare"

# Create a file for Anacostia logs
log_path = f"{leaf_test_path}/anacostia.log"
log_file_handler = logging.FileHandler(log_path, mode='a')
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


class EvalNode(BaseActionNode):
    def __init__(
        self, name: str, leaf_data_node: FilesystemStoreNode,
        loggers: Logger | List[Logger] = None
    ) -> None:
        super().__init__(name=name, predecessors=[leaf_data_node], loggers=loggers)
        self.leaf_data_node = leaf_data_node
    
    async def execute(self, *args, **kwargs) -> bool:
        self.log("Evaluating LLM on Shakespeare validation dataset", level="INFO")
        
        """
        try:
            await self.metadata_store_rpc.log_metrics(node_name=self.name, shakespeare_test_loss=1.47)
        except Exception as e:
            self.log(f"Failed to log metrics: {e}", level="ERROR")
        
        try:
            run_id = await self.metadata_store_rpc.get_run_id()
            await self.root_data_rpc.get_artifact(filepath=f"model{run_id}.txt")
        except Exception as e:
            self.log(f"Failed to get artifact: {e}", level="ERROR")
        
        try:
            num_artifacts = await self.root_data_rpc.get_num_artifacts()
            artifacts = await self.root_data_rpc.list_artifacts()
            self.log(f"{num_artifacts} Artifacts: {artifacts}", level="INFO")
        except Exception as e:
            self.log(f"Failed to list artifacts: {e}", level="ERROR")
        """
            
        return True


metadata_store_caller = SQLMetadataRPCCaller(caller_name="metadata_store_rpc")
leaf_data_node = FilesystemStoreNode(
    name="leaf_data_node", resource_path=shakespeare_input_path, metadata_store_caller=metadata_store_caller, wait_for_connection=True
)
shakespeare_eval = EvalNode(name="shakespeare_eval", leaf_data_node=leaf_data_node)

pipeline = LeafPipeline(
    name="leaf_pipeline",
    nodes=[leaf_data_node, shakespeare_eval], 
    loggers=logger
)
service = LeafPipelineServer(
    name="leaf", 
    pipeline=pipeline, 
    host=args.host, 
    port=args.port, 
    rpc_callers=[metadata_store_caller],
    logger=logger,
    uvicorn_access_log_config=LOGGING_CONFIG
)
service.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:8000", "http://localhost:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

service.run()
import os
import shutil
from typing import List
from logging.config import dictConfig
import logging
import argparse

from loggers import ROOT_ACCESS_LOGGING_CONFIG, ROOT_ANACOSTIA_LOGGING_CONFIG
from anacostia_pipeline.nodes.metadata.sql.sqlite.node import SQLiteMetadataStoreNode
from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.pipelines.pipeline import Pipeline
from anacostia_pipeline.pipelines.server import PipelineServer

from utils import *


parser = argparse.ArgumentParser()
parser.add_argument('root_host', type=str)
parser.add_argument('root_port', type=int)
parser.add_argument('leaf1_host', type=str)
parser.add_argument('leaf1_port', type=int)
args = parser.parse_args()


# Create the testing artifacts directory for the SQLAlchemy tests
metadata_store_path = f"{root_input_artifacts}/metadata_store"
data_store_path = f"{root_input_artifacts}/data_store"


dictConfig(ROOT_ANACOSTIA_LOGGING_CONFIG)
logger = logging.getLogger("root_anacostia")


# override the BaseActionNode to create a custom action node. This is just a placeholder for the actual implementation
class LoggingNode(BaseActionNode):
    def __init__(self, name: str, predecessors: List[BaseNode] = None, remote_successors: List[str] = None) -> None:
        super().__init__(name=name, predecessors=predecessors, remote_successors=remote_successors)
    
    async def execute(self, *args, **kwargs) -> bool:
        self.log("Root logging node executed", level="INFO")
        return True


# Create the nodes
metadata_store = SQLiteMetadataStoreNode(name="metadata_store", uri=f"sqlite:///{metadata_store_path}/metadata.db")
data_store = FilesystemStoreNode(name="data_store", resource_path=data_store_path, metadata_store=metadata_store)
logging_node = LoggingNode(
    name="logging_root", 
    predecessors=[data_store],
    remote_successors=[
        f"http://{args.leaf1_host}:{args.leaf1_port}/logging_leaf_1"    # http://127.0.0.1:8001/logging_leaf_1
    ]
)

# Create the pipeline
pipeline = Pipeline(
    name="logging_root", 
    nodes=[metadata_store, data_store, logging_node],
    loggers=logger
)

# Create the web server
webserver = PipelineServer(
    name="test_pipeline", pipeline=pipeline, host=args.root_host, port=args.root_port, uvicorn_access_log_config=ROOT_ACCESS_LOGGING_CONFIG, logger=logger
)
webserver.run()
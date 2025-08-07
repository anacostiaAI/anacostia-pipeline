from typing import List, Dict, Union
import logging
from logging import Logger
from logging.config import dictConfig
import argparse

from loggers import LEAF_ACCESS_LOGGING_CONFIG, LEAF_ANACOSTIA_LOGGING_CONFIG
from anacostia_pipeline.pipelines.server import PipelineServer
from anacostia_pipeline.pipelines.pipeline import Pipeline
from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.metadata.sql.api import SQLMetadataStoreClient


parser = argparse.ArgumentParser()
parser.add_argument('host', type=str)
parser.add_argument('port', type=int)
args = parser.parse_args()

path = f"./leaf-artifacts"
input_path = f"{path}/input_artifacts"
output_path = f"{path}/output_artifacts"
shakespeare_input_path = f"{input_path}/shakespeare"
shakespeare_output_path = f"{output_path}/shakespeare"


# Set up logging
dictConfig(LEAF_ANACOSTIA_LOGGING_CONFIG)
logger = logging.getLogger("leaf_anacostia")


class EvalNode(BaseActionNode):
    def __init__(
        self, name: str, leaf_data_node: FilesystemStoreNode,
        loggers: Logger | List[Logger] = None
    ) -> None:
        super().__init__(name=name, predecessors=[leaf_data_node], loggers=loggers)
        self.leaf_data_node = leaf_data_node
    
    def execute(self, *args, **kwargs) -> bool:
        self.log("Evaluating LLM on Shakespeare validation dataset", level="INFO")
        return True


metadata_store_client = SQLMetadataStoreClient(client_name="metadata_store_rpc")
leaf_data_node = FilesystemStoreNode(
    name="leaf_data_node", resource_path=shakespeare_input_path, metadata_store_client=metadata_store_client, wait_for_connection=True
)
shakespeare_eval = EvalNode(name="shakespeare_eval", leaf_data_node=leaf_data_node)

pipeline = Pipeline(
    name="leaf_pipeline",
    nodes=[leaf_data_node, shakespeare_eval], 
    loggers=logger
)
service = PipelineServer(
    name="leaf", 
    pipeline=pipeline, 
    host=args.host, 
    port=args.port, 
    remote_clients=[metadata_store_client],
    allow_origins=["http://127.0.0.1:8000", "http://localhost:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    logger=logger,
    uvicorn_access_log_config=LEAF_ACCESS_LOGGING_CONFIG
)

service.run()
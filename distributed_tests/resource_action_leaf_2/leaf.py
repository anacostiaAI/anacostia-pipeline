from typing import List, Dict, Union
import logging
from logging import Logger
from logging.config import dictConfig
import argparse

from loggers import LEAF_ACCESS_LOGGING_CONFIG, LEAF_ANACOSTIA_LOGGING_CONFIG
from anacostia_pipeline.pipelines.leaf.server import LeafPipelineServer
from anacostia_pipeline.pipelines.leaf.pipeline import LeafPipeline
from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.metadata.sql.rpc import SQLMetadataStoreClient
from anacostia_pipeline.nodes.metadata.sql.sqlite.node import SQLiteMetadataStoreNode



parser = argparse.ArgumentParser()
parser.add_argument('host', type=str)
parser.add_argument('port', type=int)
args = parser.parse_args()

path = f"./leaf-artifacts"
input_path = f"{path}/input_artifacts"
output_path = f"{path}/output_artifacts"
shakespeare_input_path = f"{input_path}/shakespeare"
shakespeare_output_path = f"{output_path}/shakespeare"
metadata_store_path = f"sqlite:///{input_path}/metadata_store/metadata.db"

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
    
    async def execute(self, *args, **kwargs) -> bool:
        try:
            current_artifacts = await self.leaf_data_node.list_artifacts("current")
            for filepath in current_artifacts:
                with open(filepath, 'r') as f:
                    self.log(f"Evaluated LLM on {filepath}", level="INFO")
        
        except Exception as e:
            self.log(f"Failed to list artifacts: {e}", level="ERROR")

        return True


metadata_store = SQLiteMetadataStoreNode(name="leaf_metadata_store", uri=metadata_store_path)
metadata_store_caller = SQLMetadataStoreClient(client_name="metadata_store_rpc", loggers=logger)
leaf_data_node = FilesystemStoreNode(
    name="leaf_data_node", 
    resource_path=shakespeare_input_path, 
    metadata_store=metadata_store, 
    metadata_store_caller=metadata_store_caller, 
    wait_for_connection=True
)
shakespeare_eval = EvalNode(name="shakespeare_eval", leaf_data_node=leaf_data_node)

pipeline = LeafPipeline(
    name="leaf_pipeline",
    nodes=[metadata_store, leaf_data_node, shakespeare_eval], 
    loggers=logger
)
service = LeafPipelineServer(
    name="leaf", 
    pipeline=pipeline, 
    host=args.host, 
    port=args.port, 
    rpc_callers=[metadata_store_caller],
    allow_origins=["http://127.0.0.1:8000", "http://localhost:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    logger=logger,
    uvicorn_access_log_config=LEAF_ACCESS_LOGGING_CONFIG
)

if __name__ == "__main__":
    service.run()
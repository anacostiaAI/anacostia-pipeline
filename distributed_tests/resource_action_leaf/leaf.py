from typing import List, Dict, Union
import logging
from logging import Logger
import argparse

from anacostia_pipeline.pipelines.leaf.server import LeafPipelineServer
from anacostia_pipeline.pipelines.leaf.pipeline import LeafPipeline
from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.metadata.sql.rpc import SQLMetadataRPCCaller
from anacostia_pipeline.nodes.resources.filesystem.rpc import FilesystemStoreRPCCaller


parser = argparse.ArgumentParser()
parser.add_argument('host', type=str)
parser.add_argument('port', type=int)
args = parser.parse_args()

leaf_test_path = "./testing_artifacts"
log_path = f"{leaf_test_path}/anacostia.log"
logging.basicConfig(
    level=logging.INFO,
    format='LEAF %(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='a'
)
logger = logging.getLogger(__name__)

path = f"./leaf-artifacts"
input_path = f"{path}/input_artifacts"
output_path = f"{path}/output_artifacts"
shakespeare_data_store_path = f"{input_path}/shakespeare"


class EvalNode(BaseActionNode):
    def __init__(
        self, name: str, metadata_store_rpc: SQLMetadataRPCCaller, root_data_rpc: FilesystemStoreRPCCaller, leaf_data_node: FilesystemStoreNode,
        loggers: Logger | List[Logger] = None
    ) -> None:
        super().__init__(name=name, predecessors=[leaf_data_node], wait_for_connection=True, loggers=loggers)
        self.metadata_store_rpc = metadata_store_rpc
        self.root_data_rpc = root_data_rpc
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


metadata_store_rpc = SQLMetadataRPCCaller(caller_name="metadata_store_rpc")
root_data_rpc = FilesystemStoreRPCCaller(caller_name="root_data_rpc", storage_directory=shakespeare_data_store_path)

leaf_data_node = FilesystemStoreNode(name="leaf_data_node", resource_path=shakespeare_data_store_path, metadata_store_rpc=metadata_store_rpc)
shakespeare_eval = EvalNode(
    name="shakespeare_eval", 
    metadata_store_rpc=metadata_store_rpc, 
    root_data_rpc=root_data_rpc, 
    leaf_data_node=leaf_data_node
)

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
    rpc_callers=[metadata_store_rpc, root_data_rpc],
    logger=logger
)
service.run()
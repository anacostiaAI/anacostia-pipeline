import argparse
import logging
from logging import Logger
from typing import List

from anacostia_pipeline.pipelines.leaf.pipeline import LeafPipeline
from anacostia_pipeline.pipelines.leaf.server import LeafPipelineServer
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.metadata.sqlite.rpc import SqliteMetadataRPCCaller



parser = argparse.ArgumentParser()
parser.add_argument('host', type=str)
parser.add_argument('port', type=int)
args = parser.parse_args()

root_test_path = "./testing_artifacts"

log_path = f"{root_test_path}/anacostia.log"
logging.basicConfig(
    level=logging.INFO,
    format='LEAF %(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='a'
)
logger = logging.getLogger(__name__)


class ShakespeareEvalNode(BaseActionNode):
    def __init__(
        self, name: str, metadata_store_rpc: SqliteMetadataRPCCaller,
        loggers: Logger | List[Logger] = None
    ) -> None:
        super().__init__(name=name, predecessors=[], wait_for_connection=True, loggers=loggers)
        self.metadata_store_rpc = metadata_store_rpc
    
    async def execute(self, *args, **kwargs) -> bool:
        self.log("Evaluating LLM on Shakespeare validation dataset", level="INFO")
        
        try:
            await self.metadata_store_rpc.log_metrics(node_name=self.name, shakespeare_test_loss=1.47)
        except Exception as e:
            self.log(f"Failed to log metrics: {e}", level="ERROR")
            
        return True

class HaikuEvalNode(BaseActionNode):
    def __init__(
        self, name: str, metadata_store_rpc: SqliteMetadataRPCCaller, 
        loggers: Logger | List[Logger] = None
    ) -> None:
        super().__init__(name=name, predecessors=[], wait_for_connection=True, loggers=loggers)
        self.metadata_store_rpc = metadata_store_rpc
    
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

        return True

metadata_store_rpc = SqliteMetadataRPCCaller("metadata_store_rpc")
shakespeare_eval = ShakespeareEvalNode("shakespeare_eval", metadata_store_rpc=metadata_store_rpc)
haiku_eval = HaikuEvalNode("haiku_eval", metadata_store_rpc=metadata_store_rpc)

pipeline = LeafPipeline(
    name="leaf_pipeline",
    nodes=[shakespeare_eval, haiku_eval],
    loggers=logger
)

service = LeafPipelineServer(name="leaf", pipeline=pipeline, host=args.host, port=args.port, rpc_callers=[metadata_store_rpc], logger=logger)
service.run()

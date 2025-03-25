import argparse
import logging
from logging import Logger
from typing import List

from anacostia_pipeline.pipelines.leaf.pipeline1 import LeafPipeline
from anacostia_pipeline.pipelines.leaf.app1 import LeafPipelineApp
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.rpc import BaseRPCCaller



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
        self, name: str, #metadata_store_rpc: BaseRPCNode, shakespeare_rpc: BaseRPCNode, 
        loggers: Logger | List[Logger] = None
    ) -> None:
        #self.metadata_store_rpc = metadata_store_rpc
        #self.shakespeare_rpc = shakespeare_rpc
        super().__init__(name=name, predecessors=[], wait_for_connection=True, loggers=loggers)
    
    def execute(self, *args, **kwargs) -> bool:
        self.log("Evaluating LLM on Shakespeare validation dataset", level="INFO")
        
        """
        try:
            self.metadata_store_rpc.log_metrics(shakespeare_test_loss=1.47)
        except Exception as e:
            self.log(f"Failed to log metrics: {e}", level="ERROR")
        """
            
        return True

class HaikuEvalNode(BaseActionNode):
    def __init__(
        self, name: str, # metadata_store_rpc: BaseRPCNode, haiku_rpc: BaseRPCNode, 
        loggers: Logger | List[Logger] = None
    ) -> None:
        #self.metadata_store_rpc = metadata_store_rpc
        #self.haiku_rpc = haiku_rpc
        super().__init__(name=name, predecessors=[], wait_for_connection=True, loggers=loggers)
    
    def execute(self, *args, **kwargs) -> bool:
        self.log("Evaluating LLM on Haiku validation dataset", level="INFO")
        # self.receiver.log_metrics(haiku_test_loss=2.43)
        return True

metadata_store_rpc = BaseRPCCaller("metadata_store_rpc", logger=logger)
shakespeare_eval = ShakespeareEvalNode("shakespeare_eval")
haiku_eval = HaikuEvalNode("haiku_eval")

pipeline = LeafPipeline(
    name="leaf_pipeline",
    nodes=[shakespeare_eval, haiku_eval],
    loggers=logger
)

service = LeafPipelineApp(name="leaf", pipeline=pipeline, host=args.host, port=args.port, rpc_callers=[metadata_store_rpc], logger=logger)
service.run()

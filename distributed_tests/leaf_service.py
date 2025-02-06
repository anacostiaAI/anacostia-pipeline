import argparse
import logging
from logging import Logger
from typing import List

from anacostia_pipeline.pipelines.leaf.pipeline import LeafPipeline
from anacostia_pipeline.pipelines.leaf.app import LeafPipelineApp
from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.network.receiver.node import ReceiverNode
from receivers import ShakespeareEvalReceiver



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
        self, name: str, receiver, predecessors: List[BaseNode], 
        loggers: Logger | List[Logger] = None
    ) -> None:
        super().__init__(name, predecessors, loggers)
        self.receiver = receiver
    
    def execute(self, *args, **kwargs) -> bool:
        self.log("Evaluating LLM on Shakespeare validation dataset", level="INFO")
        
        try:
            self.receiver.log_metrics(shakespeare_test_loss=1.47)
        except Exception as e:
            self.log(f"Failed to log metrics: {e}", level="ERROR")
            
        return True

class HaikuEvalNode(BaseActionNode):
    def __init__(
        self, name: str, predecessors: List[BaseNode], 
        loggers: Logger | List[Logger] = None
    ) -> None:
        super().__init__(name, predecessors, loggers)
    
    def execute(self, *args, **kwargs) -> bool:
        self.log("Evaluating LLM on Haiku validation dataset", level="INFO")
        #self.metadata_store.log_metrics(haiku_test_loss=2.43)
        return True

shakespeare_eval_receiver = ShakespeareEvalReceiver("shakespeare_eval_receiver", loggers=[])
haiku_eval_receiver = ReceiverNode("haiku_eval_receiver", loggers=[])
shakespeare_eval = ShakespeareEvalNode("shakespeare_eval", receiver=shakespeare_eval_receiver, predecessors=[shakespeare_eval_receiver])
haiku_eval = HaikuEvalNode("haiku_eval", predecessors=[haiku_eval_receiver])

pipeline = LeafPipeline(
    name="leaf_pipeline",
    nodes=[shakespeare_eval, haiku_eval, shakespeare_eval_receiver, haiku_eval_receiver],
    loggers=logger
)

service = LeafPipelineApp(name="leaf", pipeline=pipeline, host=args.host, port=args.port, logger=logger)
service.run()

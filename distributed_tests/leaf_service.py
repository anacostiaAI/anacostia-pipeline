import argparse
import logging
from logging import Logger
from typing import List

from anacostia_pipeline.engine.pipeline import LeafPipeline
from anacostia_pipeline.dashboard.service import LeafService
from anacostia_pipeline.dashboard.subapps.pipeline import LeafPipelineWebserver
from anacostia_pipeline.engine.base import BaseNode, BaseActionNode
from anacostia_pipeline.actions.network import ReceiverNode



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
        self, name: str, predecessors: List[BaseNode], 
        loggers: Logger | List[Logger] = None
    ) -> None:
        super().__init__(name, predecessors, loggers)
    
    def execute(self, *args, **kwargs) -> bool:
        self.log("Evaluating LLM on Shakespeare validation dataset", level="INFO")
        #self.metadata_store.log_metrics(shakespeare_test_loss=1.47)
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

shakespeare_eval_receiver = ReceiverNode("shakespeare_eval_receiver", loggers=[])
haiku_eval_receiver = ReceiverNode("haiku_eval_receiver", loggers=[])

shakespeare_eval = ShakespeareEvalNode("shakespeare_eval", predecessors=[shakespeare_eval_receiver])
haiku_eval = HaikuEvalNode("haiku_eval", predecessors=[haiku_eval_receiver])


pipeline = LeafPipeline(
    name="leaf_pipeline",
    nodes=[shakespeare_eval, haiku_eval, shakespeare_eval_receiver, haiku_eval_receiver],
    loggers=logger
)
webserver = LeafPipelineWebserver(name="leaf", pipeline=pipeline, host=args.host, port=args.port, logger=logger)
webserver.run()

#service = LeafService(name="leaf", pipeline=pipeline, host=args.host, port=args.port, logger=logger)
#service.run()


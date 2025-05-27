from logging.config import dictConfig
import logging
import argparse

from loggers import LEAF_ACCESS_LOGGING_CONFIG_2, LEAF_ANACOSTIA_LOGGING_CONFIG_2
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.pipelines.pipeline import Pipeline
from anacostia_pipeline.pipelines.server import PipelineServer


parser = argparse.ArgumentParser()
parser.add_argument('leaf2_host', type=str)
parser.add_argument('leaf2_port', type=int)
args = parser.parse_args()


dictConfig(LEAF_ANACOSTIA_LOGGING_CONFIG_2)
logger = logging.getLogger("leaf_anacostia_2")


# override the BaseActionNode to create a custom action node. This is just a placeholder for the actual implementation
class LoggingNode(BaseActionNode):
    def __init__(self, name: str) -> None:
        super().__init__(name=name, predecessors=[], wait_for_connection=True)
    
    async def execute(self, *args, **kwargs) -> bool:
        self.log("Leaf-2 logging node executed", level="INFO")
        return True


logging_node = LoggingNode("logging_leaf_2")
pipeline = Pipeline(name="leaf2", nodes=[logging_node], loggers=logger)
server = PipelineServer(
    "leaf2_server", pipeline=pipeline, host=args.leaf2_host, port=args.leaf2_port, logger=logger, uvicorn_access_log_config=LEAF_ACCESS_LOGGING_CONFIG_2
)
server.run()
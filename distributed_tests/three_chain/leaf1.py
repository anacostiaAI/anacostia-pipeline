from logging.config import dictConfig
import logging
import argparse
from typing import List

from loggers import LEAF_ACCESS_LOGGING_CONFIG_1, LEAF_ANACOSTIA_LOGGING_CONFIG_1
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.pipelines.pipeline import Pipeline
from anacostia_pipeline.pipelines.server import PipelineServer


parser = argparse.ArgumentParser()
parser.add_argument('leaf1_host', type=str)
parser.add_argument('leaf1_port', type=int)
parser.add_argument('leaf2_host', type=str)
parser.add_argument('leaf2_port', type=int)
args = parser.parse_args()


dictConfig(LEAF_ANACOSTIA_LOGGING_CONFIG_1)
logger = logging.getLogger("leaf_anacostia_1")


# override the BaseActionNode to create a custom action node. This is just a placeholder for the actual implementation
class LoggingNode(BaseActionNode):
    def __init__(self, name: str, remote_successors: List[str]) -> None:
        super().__init__(name=name, predecessors=[], remote_successors=remote_successors, wait_for_connection=True)
    
    async def execute(self, *args, **kwargs) -> bool:
        self.log("Leaf-1 logging node executed", level="INFO")
        return True


logging_node = LoggingNode(
    "logging_leaf_1", 
    remote_successors=[
        f"http://{args.leaf2_host}:{args.leaf2_port}/logging_leaf_2"    # http://127.0.0.1:8002/logging_leaf_2
    ]
)
pipeline = Pipeline(name="leaf1", nodes=[logging_node], loggers=logger)
server = PipelineServer(
    "leaf1_server", pipeline=pipeline, host=args.leaf1_host, port=args.leaf1_port, logger=logger, uvicorn_access_log_config=LEAF_ACCESS_LOGGING_CONFIG_1
)
server.run()
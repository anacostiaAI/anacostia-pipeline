import os
import shutil
from typing import List
from pathlib import Path

from anacostia_pipeline.nodes.metadata.sql.sqlite.node import SQLiteMetadataStoreNode
from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.pipelines.pipeline import Pipeline
from anacostia_pipeline.pipelines.server import PipelineServer

import logging
from loggers import LEAF_ANACOSTIA_LOGGING_CONFIG, LEAF_ACCESS_LOGGING_CONFIG
from logging.config import dictConfig
from logging import Logger
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('host', type=str)
parser.add_argument('port', type=int)
args = parser.parse_args()


dictConfig(LEAF_ANACOSTIA_LOGGING_CONFIG)
logger = logging.getLogger("leaf_anacostia")


mkcert_ca = Path(os.popen("mkcert -CAROOT").read().strip()) / "rootCA.pem"
mkcert_ca = str(mkcert_ca)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ssl_certfile = os.path.join(BASE_DIR, "certs/certificate_root.pem")
ssl_keyfile = os.path.join(BASE_DIR, "certs/private_root.key")


class ShakespeareEvalNode(BaseActionNode):
    def __init__(
        self, name: str, 
        loggers: Logger | List[Logger] = None
    ) -> None:
        super().__init__(name=name, predecessors=[], wait_for_connection=True, loggers=loggers)
    
    async def execute(self, *args, **kwargs) -> bool:
        self.log("Evaluating LLM on Shakespeare validation dataset", level="INFO")


shakespeare_eval = ShakespeareEvalNode("shakespeare_eval", loggers=logger)

pipeline = Pipeline(name="shakespeare_eval_pipeline", nodes=[shakespeare_eval], loggers=[logger])

webserver = PipelineServer(
    name="shakespeare_eval_pipeline",
    pipeline=pipeline,
    host=args.host, 
    port=args.port,
    logger=logger, uvicorn_access_log_config=LEAF_ACCESS_LOGGING_CONFIG,
    #ssl_ca_certs=mkcert_ca,
    #ssl_certfile=ssl_certfile,
    #ssl_keyfile=ssl_keyfile
)
webserver.run()
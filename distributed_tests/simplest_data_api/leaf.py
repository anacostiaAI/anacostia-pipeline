import os
from typing import List
from pathlib import Path

from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.metadata.sql.api import SQLMetadataStoreClient
from anacostia_pipeline.pipelines.pipeline import Pipeline
from anacostia_pipeline.pipelines.server import PipelineServer, AnacostiaServer

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
ssl_certfile = os.path.join(BASE_DIR, "certs/certificate_leaf.pem")
ssl_keyfile = os.path.join(BASE_DIR, "certs/private_leaf.key")


class ShakespeareEvalNode(BaseActionNode):
    def __init__(
        self, name: str, 
        metadata_store_rpc: SQLMetadataStoreClient,
        loggers: Logger | List[Logger] = None
    ) -> None:
        super().__init__(name=name, predecessors=[], wait_for_connection=True, loggers=loggers)
        self.metadata_store_rpc = metadata_store_rpc

    def execute(self, *args, **kwargs) -> bool:
        self.log("Evaluating LLM on Shakespeare validation dataset", level="INFO")
        try:
            self.metadata_store_rpc.log_metrics(node_name=self.name, shakespeare_test_loss=1.47)

            metrics = self.metadata_store_rpc.get_metrics(node_name=self.name)
            self.log(f"Metrics logged: {metrics}", level="INFO")
        except Exception as e:
            self.log(f"Failed to log metrics: {e}", level="ERROR")
        return True


metadata_store_rpc = SQLMetadataStoreClient(client_name="metadata_store_rpc")
shakespeare_eval = ShakespeareEvalNode("shakespeare_eval", metadata_store_rpc=metadata_store_rpc)

pipeline = Pipeline(name="shakespeare_eval_pipeline", nodes=[shakespeare_eval], loggers=[logger])

service = PipelineServer(
    name="shakespeare_eval_pipeline",
    pipeline=pipeline,
    host=args.host, 
    port=args.port,
    remote_clients=[metadata_store_rpc], 
    ssl_ca_certs=mkcert_ca,
    ssl_certfile=ssl_certfile,
    ssl_keyfile=ssl_keyfile,
    logger=logger, 
    uvicorn_access_log_config=LEAF_ACCESS_LOGGING_CONFIG
)

config = service.get_config()
server = AnacostiaServer(config=config)

with server.run_in_thread():
    while True:
        try:
            pass    # Keep the server running
        except (KeyboardInterrupt, SystemExit):
            print("Shutting down the server...")
            break
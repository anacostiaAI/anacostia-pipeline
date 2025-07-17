from typing import List
import argparse
import os
from pathlib import Path

from anacostia_pipeline.nodes.metadata.sql.sqlite.node import SQLiteMetadataStoreNode
from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.pipelines.pipeline import Pipeline
from anacostia_pipeline.pipelines.server import PipelineServer

from loggers import ROOT_ANACOSTIA_LOGGING_CONFIG, ROOT_ACCESS_LOGGING_CONFIG
from logging.config import dictConfig
import logging



parser = argparse.ArgumentParser()
parser.add_argument('root_host', type=str)
parser.add_argument('root_port', type=int)
parser.add_argument('leaf_host', type=str)
parser.add_argument('leaf_port', type=int)
args = parser.parse_args()

# Create the testing artifacts directory for the SQLAlchemy tests
tests_path = "./testing_artifacts"
metadata_store_path = f"{tests_path}/metadata_store"
data_store_path = f"{tests_path}/data_store"

dictConfig(ROOT_ANACOSTIA_LOGGING_CONFIG)
logger = logging.getLogger("root_anacostia")

mkcert_ca = Path(os.popen("mkcert -CAROOT").read().strip()) / "rootCA.pem"
mkcert_ca = str(mkcert_ca)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ssl_certfile = os.path.join(BASE_DIR, "certs/certificate_root.pem")
ssl_keyfile = os.path.join(BASE_DIR, "certs/private_root.key")



# override the BaseActionNode to create a custom action node. This is just a placeholder for the actual implementation
class PrintingNode(BaseActionNode):
    def __init__(self, name: str, predecessors: List[BaseNode] = None, remote_successors: List[str] = None) -> None:
        super().__init__(name=name, predecessors=predecessors, remote_successors=remote_successors)

    async def execute(self, *args, **kwargs) -> bool:
        self.log("Logging node executed", level="INFO")
        return True

# Create the nodes
metadata_store = SQLiteMetadataStoreNode(
    name="metadata_store", 
    uri=f"sqlite:///{metadata_store_path}/metadata.db",
    client_url=f"https://{args.leaf_host}:{args.leaf_port}/metadata_store_rpc"
)
data_store = FilesystemStoreNode(name="data_store", resource_path=data_store_path, metadata_store=metadata_store)
printing_node = PrintingNode(
    "logging_node", 
    predecessors=[data_store], 
    remote_successors=[f"https://{args.leaf_host}:{args.leaf_port}/shakespeare_eval"]
)

# Create the pipeline
pipeline = Pipeline(name="test_pipeline", nodes=[metadata_store, data_store, printing_node], loggers=[logger])

# Create the web server
webserver = PipelineServer(
    name="test_pipeline", 
    pipeline=pipeline, 
    host=args.root_host, 
    port=args.root_port,
    ssl_ca_certs=mkcert_ca,
    ssl_certfile=ssl_certfile,
    ssl_keyfile=ssl_keyfile,
    logger=logger,
    uvicorn_access_log_config=ROOT_ACCESS_LOGGING_CONFIG
)
webserver.run()
from typing import List
from logging.config import dictConfig
import logging
import argparse
from pathlib import Path

from loggers import ROOT_ACCESS_LOGGING_CONFIG, ROOT_ANACOSTIA_LOGGING_CONFIG
from anacostia_pipeline.nodes.metadata.sql.sqlite.node import SQLiteMetadataStoreNode
from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.node import BaseNode

from anacostia_pipeline.pipelines.pipeline import Pipeline
from anacostia_pipeline.pipelines.server import PipelineServer, AnacostiaServer

from utils import *


parser = argparse.ArgumentParser()
parser.add_argument('root_host', type=str)
parser.add_argument('root_port', type=int)
parser.add_argument('leaf1_host', type=str)
parser.add_argument('leaf1_port', type=int)
parser.add_argument('leaf2_host', type=str)
parser.add_argument('leaf2_port', type=int)
args = parser.parse_args()


# Create the testing artifacts directory for the SQLAlchemy tests
metadata_store_path = f"{root_input_artifacts}/metadata_store"
data_store_path = f"{root_input_artifacts}/data_store"


dictConfig(ROOT_ANACOSTIA_LOGGING_CONFIG)
logger = logging.getLogger("root_anacostia")

mkcert_ca = Path(os.popen("mkcert -CAROOT").read().strip()) / "rootCA.pem"
mkcert_ca = str(mkcert_ca)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ssl_certfile = os.path.join(BASE_DIR, "certs/certificate_root.pem")
ssl_keyfile = os.path.join(BASE_DIR, "certs/private_root.key")



# override the BaseActionNode to create a custom action node. This is just a placeholder for the actual implementation
class LoggingNode(BaseActionNode):
    def __init__(self, name: str, predecessors: List[BaseNode] = None, remote_successors: List[str] = None) -> None:
        super().__init__(name=name, predecessors=predecessors, remote_successors=remote_successors)
    
    def execute(self, *args, **kwargs) -> bool:
        self.log("Root logging node executed", level="INFO")
        return True


# Create the nodes
metadata_store = SQLiteMetadataStoreNode(name="metadata_store", uri=f"sqlite:///{metadata_store_path}/metadata.db")
data_store = FilesystemStoreNode(name="data_store", resource_path=data_store_path, metadata_store=metadata_store)
logging_node = LoggingNode(
    name="logging_root", 
    predecessors=[data_store],
    remote_successors=[
        f"https://{args.leaf1_host}:{args.leaf1_port}/logging_leaf_1",   # https://127.0.0.1:8001/logging_leaf_1
        f"https://{args.leaf2_host}:{args.leaf2_port}/logging_leaf_2"    # https://127.0.0.1:8002/logging_leaf_2
    ]
)

# Create the pipeline
pipeline = Pipeline(
    name="root_pipeline",
    nodes=[metadata_store, data_store, logging_node],
    loggers=logger
)

# Create the web server
service = PipelineServer(
    name="test_pipeline",
    pipeline=pipeline,
    host=args.root_host,
    port=args.root_port,
    uvicorn_access_log_config=ROOT_ACCESS_LOGGING_CONFIG,
    logger=logger,
    allow_origins=["https://127.0.0.1:8000", "https://localhost:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    ssl_ca_certs=mkcert_ca,
    ssl_certfile=ssl_certfile,
    ssl_keyfile=ssl_keyfile,
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

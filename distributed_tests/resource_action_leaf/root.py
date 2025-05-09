import logging
from logging.config import dictConfig
import argparse

from loggers import ROOT_ANACOSTIA_LOGGING_CONFIG, ROOT_ACCESS_LOGGING_CONFIG
from anacostia_pipeline.pipelines.root.pipeline import RootPipeline
from anacostia_pipeline.pipelines.root.server import RootPipelineServer
from anacostia_pipeline.nodes.metadata.sql.sqlite.node import SQLiteMetadataStoreNode


parser = argparse.ArgumentParser()
parser.add_argument('root_host', type=str)
parser.add_argument('root_port', type=int)
parser.add_argument('leaf_host', type=str)
parser.add_argument('leaf_port', type=int)
args = parser.parse_args()

path = f"./root-artifacts"
input_path = f"{path}/input_artifacts"
output_path = f"{path}/output_artifacts"
metadata_store_path = f"{input_path}/metadata_store"
haiku_data_store_path = f"{input_path}/haiku"

dictConfig(ROOT_ANACOSTIA_LOGGING_CONFIG)
logger = logging.getLogger("root_anacostia")

metadata_store = SQLiteMetadataStoreNode(
    name="metadata_store", 
    uri=f"sqlite:///{metadata_store_path}/metadata.db",
    client_url=f"http://{args.leaf_host}:{args.leaf_port}/metadata_store_rpc",
    remote_successors=[f"http://{args.leaf_host}:{args.leaf_port}/leaf_data_node"]
)
pipeline = RootPipeline(
    nodes=[metadata_store],
    loggers=logger
)

service = RootPipelineServer(
    name="root", pipeline=pipeline, host=args.root_host, port=args.root_port, logger=logger, uvicorn_access_log_config=ROOT_ACCESS_LOGGING_CONFIG
)
service.run()
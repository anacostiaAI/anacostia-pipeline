import logging
from logging.config import dictConfig
import argparse
from pathlib import Path
import os

from loggers import ROOT_ANACOSTIA_LOGGING_CONFIG, ROOT_ACCESS_LOGGING_CONFIG
from anacostia_pipeline.pipelines.pipeline import Pipeline
from anacostia_pipeline.pipelines.server import PipelineServer
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

mkcert_ca = Path(os.popen("mkcert -CAROOT").read().strip()) / "rootCA.pem"
mkcert_ca = str(mkcert_ca)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ssl_certfile = os.path.join(BASE_DIR, "certs/certificate_leaf.pem")
ssl_keyfile = os.path.join(BASE_DIR, "certs/private_leaf.key")



metadata_store = SQLiteMetadataStoreNode(
    name="metadata_store", 
    uri=f"sqlite:///{metadata_store_path}/metadata.db",
    client_url=f"https://{args.leaf_host}:{args.leaf_port}/metadata_store_rpc",
    remote_successors=[f"https://{args.leaf_host}:{args.leaf_port}/leaf_data_node"]
)
pipeline = Pipeline(
    name="root_pipeline", 
    nodes=[metadata_store],
    loggers=logger
)

service = PipelineServer(
    name="root", 
    pipeline=pipeline, 
    host=args.root_host, 
    port=args.root_port, 
    logger=logger, 
    uvicorn_access_log_config=ROOT_ACCESS_LOGGING_CONFIG,
    ssl_ca_certs=mkcert_ca,
    ssl_certfile=ssl_certfile,
    ssl_keyfile=ssl_keyfile
)
service.run()
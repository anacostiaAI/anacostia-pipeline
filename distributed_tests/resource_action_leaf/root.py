import logging
import argparse

from anacostia_pipeline.pipelines.root.pipeline import RootPipeline
from anacostia_pipeline.pipelines.root.server import RootPipelineServer
from anacostia_pipeline.nodes.metadata.sql.sqlite.node import SQLiteMetadataStoreNode


parser = argparse.ArgumentParser()
parser.add_argument('root_host', type=str)
parser.add_argument('root_port', type=int)
parser.add_argument('leaf_host', type=str)
parser.add_argument('leaf_port', type=int)
args = parser.parse_args()

root_test_path = "./testing_artifacts"

# Create a file for Anacostia logs
log_path = f"{root_test_path}/anacostia.log"
log_file_handler = logging.FileHandler(log_path, mode='a')
log_file_handler.setLevel(logging.INFO)
log_formatter = logging.Formatter(
    fmt='ROOT %(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log_file_handler.setFormatter(log_formatter)
logger = logging.getLogger(__name__)
logger.addHandler(log_file_handler)
logger.setLevel(logging.INFO)  # Make sure it's at least INFO


# Step 1: Create a file handler for access logs
access_log_path = f"{root_test_path}/access.log"

# Uvicorn needs a dictionary describing the log setup
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "fmt": "ROOT ACCESS %(asctime)s - %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "access_file_handler": {
            "level": "INFO",
            "class": "logging.FileHandler",
            "formatter": "default",
            "filename": f"{access_log_path}",   # access_log_path = "./testing_artifacts/access.log" , log_path = "./testing_artifacts/anacostia.log"
        },
    },
    "loggers": {
        "uvicorn.access": {
            "handlers": ["access_file_handler"],
            "level": "INFO",
            "propagate": False,
        },
    },
}

path = f"./root-artifacts"
input_path = f"{path}/input_artifacts"
output_path = f"{path}/output_artifacts"
metadata_store_path = f"{input_path}/metadata_store"
haiku_data_store_path = f"{input_path}/haiku"

metadata_store = SQLiteMetadataStoreNode(
    name="metadata_store", 
    uri=f"sqlite:///{metadata_store_path}/metadata.db",
    caller_url=f"http://{args.leaf_host}:{args.leaf_port}/metadata_store_rpc",
    remote_successors=[f"http://{args.leaf_host}:{args.leaf_port}/leaf_data_node"]
)
pipeline = RootPipeline(
    nodes=[metadata_store],
    loggers=logger
)

service = RootPipelineServer(name="root", pipeline=pipeline, host=args.root_host, port=args.root_port, logger=logger, uvicorn_access_log_config=LOGGING_CONFIG)
service.run()
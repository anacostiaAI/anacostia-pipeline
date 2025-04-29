import logging
import argparse

from anacostia_pipeline.pipelines.root.pipeline import RootPipeline
from anacostia_pipeline.pipelines.root.server import RootPipelineServer
from anacostia_pipeline.nodes.metadata.sql.sqlite.node import SQLiteMetadataStoreNode
from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode


parser = argparse.ArgumentParser()
parser.add_argument('root_host', type=str)
parser.add_argument('root_port', type=int)
parser.add_argument('leaf_host', type=str)
parser.add_argument('leaf_port', type=int)
args = parser.parse_args()

root_test_path = "./testing_artifacts"

log_path = f"{root_test_path}/anacostia.log"
logging.basicConfig(
    level=logging.INFO,
    format='ROOT %(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='a'
)

uvicorn_access_logger = logging.getLogger("uvicorn.access")
uvicorn_access_logger.setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

path = f"./root-artifacts"
input_path = f"{path}/input_artifacts"
output_path = f"{path}/output_artifacts"
metadata_store_path = f"{input_path}/metadata_store"
haiku_data_store_path = f"{input_path}/haiku"


metadata_store = SQLiteMetadataStoreNode(
    name="metadata_store", 
    uri=f"sqlite:///{metadata_store_path}/metadata.db",
    caller_url=f"http://{args.leaf_host}:{args.leaf_port}/metadata_store_rpc",
    remote_successors=[f"http://{args.leaf_host}:{args.leaf_port}/leaf_data_node", f"http://{args.leaf_host}:{args.leaf_port}/shakespeare_eval"]
)
haiku_data_store = FilesystemStoreNode(
    name="haiku_data_store",
    resource_path=haiku_data_store_path,
    metadata_store=metadata_store,
    init_state="new",
    max_old_samples=None,
    caller_url=f"http://{args.leaf_host}:{args.leaf_port}/root_data_rpc",
    remote_successors=[f"http://{args.leaf_host}:{args.leaf_port}/shakespeare_eval"]
)

pipeline = RootPipeline(
    nodes=[metadata_store, haiku_data_store],
    loggers=logger
)

service = RootPipelineServer(name="root", pipeline=pipeline, host=args.root_host, port=args.root_port, logger=logger)
service.run()
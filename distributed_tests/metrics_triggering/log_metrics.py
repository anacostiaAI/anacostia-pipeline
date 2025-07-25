import time
import asyncio
import logging
import os
from pathlib import Path
from logging.config import dictConfig
from loggers import ROOT_ANACOSTIA_LOGGING_CONFIG

from anacostia_pipeline.nodes.metadata.sql.api import SQLMetadataStoreClient


dictConfig(ROOT_ANACOSTIA_LOGGING_CONFIG)
logger = logging.getLogger("root_anacostia")

mkcert_ca = Path(os.popen("mkcert -CAROOT").read().strip()) / "rootCA.pem"
mkcert_ca = str(mkcert_ca)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ssl_certfile = os.path.join(BASE_DIR, "certs/certificate_leaf.pem")
ssl_keyfile = os.path.join(BASE_DIR, "certs/private_leaf.key")

# Create an event loop in the main thread and set it
# Make sure you are not already in an event loop.
# Two event loops cannot run simultaneously in the same thread in Python.
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

# make sure metadata_store_name is the same as the name of the metadata store in the pipeline
metadata_store_name = "metadata_store"
node_name = "edge_deployment_client"
metadata_store_client = SQLMetadataStoreClient(
    client_name=node_name, 
    server_url=f"https://127.0.0.1:8000/{metadata_store_name}/api/server",
    ssl_ca_certs=mkcert_ca,
    ssl_certfile=ssl_certfile,
    ssl_keyfile=ssl_keyfile,
    loggers=logger
)
metadata_store_client.set_event_loop(loop)      # register the event loop with the metadata store client


# Note: This is a simplified example of how you might log metrics to the metadata store via the metadata store client.
# In an actual implementation, you could use the metadata_store_client to interact with the metadata store from anywhere, 
# i.e., from inside a FastAPI server where your model is running.
def run_test():
    logger.info(f"Registered remote client '{node_name}' with metadata store '{metadata_store_name}'")
    metadata_store_client.add_node(
        node_name=node_name, 
        node_type=type(metadata_store_client).__name__, 
        base_type="BaseMetadataStoreClient"
    )

    for i in range(10):
        percent_accuracy = i / 10
        metadata_store_client.log_metrics(node_name=node_name, percent_accuracy=percent_accuracy)
        logger.info(f"Logged metric percent_accuracy={percent_accuracy} to {node_name}")
        time.sleep(1.5)

async def main():
    await loop.run_in_executor(None, run_test)

try:
    loop.run_until_complete(main())
except (KeyboardInterrupt, SystemExit):
    loop.close()
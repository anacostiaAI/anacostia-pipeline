import time
import asyncio
import logging
from logging.config import dictConfig
from loggers import ROOT_ANACOSTIA_LOGGING_CONFIG

from anacostia_pipeline.nodes.metadata.sql.api import SQLMetadataStoreClient


dictConfig(ROOT_ANACOSTIA_LOGGING_CONFIG)
logger = logging.getLogger("root_anacostia")

# make sure metadata_store_name is the same as the name of the metadata store in the pipeline
metadata_store_name = "metadata_store"
node_name = "edge_deployment"
metadata_store_client = SQLMetadataStoreClient(
    client_name="metadata_store_client", 
    server_url=f"http://127.0.0.1:8000/{metadata_store_name}/api/server",
    loggers=logger
)


# Note: This is a simplified example of how you might log metrics to the metadata store via the metadata store client.
# In an actual implementation, you could use the metadata_store_client to interact with the metadata store from anywhere, 
# i.e., from inside a FastAPI server where your model is running.
async def run_test():
    await metadata_store_client.add_node(node_name=node_name, node_type="metadata_store_client")
    logger.info(f"Added node {node_name} to metadata store client")

    for i in range(10):
        percent_accuracy = i / 10
        await metadata_store_client.log_metrics(node_name=node_name, percent_accuracy=percent_accuracy)
        time.sleep(1.5)

# Make sure if you are calling asyncio.run() from a script, you are not already in an event loop.
# Two event loops cannot run simultaneously in the same thread in Python.
asyncio.run(run_test())
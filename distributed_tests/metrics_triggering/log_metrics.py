import time
import asyncio
import logging
import os
from pathlib import Path
from logging.config import dictConfig
from loggers import LEAF_ANACOSTIA_LOGGING_CONFIG

from anacostia_pipeline.nodes.metadata.sql.api import SQLMetadataStoreClient


dictConfig(LEAF_ANACOSTIA_LOGGING_CONFIG)
logger = logging.getLogger("leaf_anacostia")

mkcert_ca = Path(os.popen("mkcert -CAROOT").read().strip()) / "rootCA.pem"
mkcert_ca = str(mkcert_ca)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ssl_certfile = os.path.join(BASE_DIR, "certs/certificate_leaf.pem")
ssl_keyfile = os.path.join(BASE_DIR, "certs/private_leaf.key")


# make sure metadata_store_name is the same as the name of the metadata store in the pipeline
# Note: we don't have to set the following arguments:
# allow_origins=["https://127.0.0.1:8000", "https://localhost:8000"],
# allow_credentials=True,
# allow_methods=["*"],
# allow_headers=["*"]
# in the SQLMetadataStoreClient because it is not sending any html snippets to the client.
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

# Note: This is a simplified example of how you might log metrics to the metadata store via the metadata store client.
# In an actual implementation, you could use the metadata_store_client to interact with the metadata store from anywhere, 
# i.e., from inside a FastAPI server where your model is running.
for i in range(10):
    percent_accuracy = i / 10
    metadata_store_client.log_metrics(node_name=node_name, percent_accuracy=percent_accuracy)
    logger.info(f"Logged metric percent_accuracy={percent_accuracy} to {node_name}")
    time.sleep(1.5)

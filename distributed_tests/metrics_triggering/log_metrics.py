import time
import asyncio
from anacostia_pipeline.nodes.metadata.sql.api import SQLMetadataStoreClient


metadata_store_node_name = "metadata_store"
metadata_store_client = SQLMetadataStoreClient(client_name="metadata_store_client", server_url=f"http://127.0.0.1:8000/{metadata_store_node_name}/rpc/server")


async def run_test():
    for i in range(10):
        percent_accuracy = i / 10
        await metadata_store_client.log_metrics(node_name=metadata_store_node_name, percent_accuracy=percent_accuracy)
        time.sleep(1.5)


asyncio.run(run_test())
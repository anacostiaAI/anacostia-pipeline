import os
import shutil
from typing import List

from anacostia_pipeline.nodes.metadata.sql.sqlite.node import SQLiteMetadataStoreNode
from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.pipelines.pipeline import Pipeline
from anacostia_pipeline.pipelines.server import PipelineServer

# Create the testing artifacts directory for the SQLAlchemy tests
tests_path = "./testing_artifacts"
if os.path.exists(tests_path) is True:
    shutil.rmtree(tests_path)
os.makedirs(tests_path)
metadata_store_path = f"{tests_path}/metadata_store"
data_store_path = f"{tests_path}/data_store"

# override the BaseActionNode to create a custom action node. This is just a placeholder for the actual implementation
class PrintingNode(BaseActionNode):
    def __init__(self, name: str, predecessors: List[BaseNode] = None) -> None:
        super().__init__(name=name, predecessors=predecessors)
    
    async def execute(self, *args, **kwargs) -> bool:
        print("Logging node executed")
        return True

# Create the nodes
metadata_store = SQLiteMetadataStoreNode(name="metadata_store", uri=f"sqlite:///{metadata_store_path}/metadata.db")
data_store = FilesystemStoreNode(name="data_store", resource_path=data_store_path, metadata_store=metadata_store)
printing_node = PrintingNode("logging_node", predecessors=[data_store])

# Create the pipeline
pipeline = Pipeline(name="test_pipeline", nodes=[metadata_store, data_store, printing_node])

# Create the web server
webserver = PipelineServer(name="test_pipeline", pipeline=pipeline, host="127.0.0.1", port=8000)
webserver.run()
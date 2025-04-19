import os
import logging
import shutil
from typing import List

from anacostia_pipeline.nodes.metadata.sql.sqlite.node import SQLiteMetadataStoreNode
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.pipelines.root.pipeline import RootPipeline
from anacostia_pipeline.pipelines.root.server import RootPipelineServer


tests_path = "./testing_artifacts/sqlalchemy_tests"
if os.path.exists(tests_path) is True:
    shutil.rmtree(tests_path)
os.makedirs(tests_path)
log_path = f"{tests_path}/anacostia.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='a'
)
logger = logging.getLogger(__name__)

metadata_store_path = f"{tests_path}/metadata_store"
data_store_path = f"{tests_path}/data_store"

class LoggingNode(BaseActionNode):
    def __init__(
        self, name: str, metadata_store: BaseMetadataStoreNode, predecessors: List[BaseNode] = None
    ) -> None:
        super().__init__(name=name, predecessors=predecessors, wait_for_connection=True)
        self.metadata_store = metadata_store
    
    async def execute(self, *args, **kwargs) -> bool:
        self.log("Logging node executed", level="INFO")
        return True

metadata_store = SQLiteMetadataStoreNode(name="metadata_store", uri=f"sqlite:///{metadata_store_path}/metadata.db", loggers=[logger])
data_store = FilesystemStoreNode(name="data_store", resource_path=data_store_path, metadata_store=metadata_store, loggers=[logger])
logging_node = LoggingNode("logging_node", metadata_store=metadata_store, predecessors=[data_store])
pipeline = RootPipeline(nodes=[metadata_store, data_store, logging_node], loggers=logger)



if __name__ == "__main__":
    webserver = RootPipelineServer(name="test_pipeline", pipeline=pipeline, host="127.0.0.1", port=8000, logger=logger)
    webserver.run()

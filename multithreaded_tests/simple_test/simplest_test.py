import os
import logging
import shutil
from typing import List

from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode

from anacostia_pipeline.pipelines.root.pipeline import RootPipeline
from anacostia_pipeline.pipelines.root.server import RootPipelineServer

from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.metadata.sqlite.node import SqliteMetadataStoreNode



logging_tests_path = "./testing_artifacts/logging_tests"
if os.path.exists(logging_tests_path) is True:
    shutil.rmtree(logging_tests_path)
os.makedirs(logging_tests_path)

log_path = f"{logging_tests_path}/anacostia.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='a'
)
logger = logging.getLogger(__name__)


metadata_store_path = f"{logging_tests_path}/metadata_store"
data_store_path = f"{logging_tests_path}/data_store"


class MonitoringDataStoreNode(FilesystemStoreNode):
    def __init__(
        self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, 
        init_state: str = "new", max_old_samples: int = None
    ) -> None:
        super().__init__(name, resource_path, metadata_store, init_state, max_old_samples)

class LoggingNode(BaseActionNode):
    def __init__(
        self, name: str, metadata_store: BaseMetadataStoreNode, predecessors: List[BaseNode] = None
    ) -> None:
        super().__init__(name=name, predecessors=predecessors, wait_for_connection=True)
        self.metadata_store = metadata_store
    
    async def execute(self, *args, **kwargs) -> bool:
        self.log("Logging node executed", level="INFO")
        return True

metadata_store = SqliteMetadataStoreNode(name="metadata_store", uri=f"{metadata_store_path}/metadata.db")
data_store = MonitoringDataStoreNode("data_store", data_store_path, metadata_store)
logging_node = LoggingNode("logging_node", metadata_store=metadata_store, predecessors=[data_store])

pipeline = RootPipeline(nodes=[metadata_store, data_store, logging_node], loggers=logger)

if __name__ == "__main__":
    webserver = RootPipelineServer(name="test_pipeline", pipeline=pipeline, host="127.0.0.1", port=8000, logger=logger)
    webserver.run()
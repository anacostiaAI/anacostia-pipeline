import os
import shutil
import logging

from anacostia_pipeline.nodes.metadata.sql.sqlite.node import SQLiteMetadataStoreNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.resources.node import BaseResourceNode
from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.pipelines.pipeline import Pipeline
from anacostia_pipeline.pipelines.server import PipelineServer, AnacostiaServer

from data_store import DatasetStoreNode



# Create the testing artifacts directory for the SQLAlchemy tests
tests_path = "./testing_artifacts"
if os.path.exists(tests_path) is True:
    shutil.rmtree(tests_path)
os.makedirs(tests_path)
metadata_store_path = f"{tests_path}/metadata_store"
data_store_path = f"{tests_path}/data_store"
dataset_store_path = f"{tests_path}/dataset_store"

log_path = f"{tests_path}/anacostia.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='a'
)
logger = logging.getLogger(__name__)



class MonitoringDataStoreNode(FilesystemStoreNode):
    def __init__(self, name, resource_path, metadata_store):
        super().__init__(name, resource_path, metadata_store, monitoring=True)
    
    def resource_trigger(self) -> None:
        num_new_artifacts = self.get_num_artifacts("new")
        if num_new_artifacts is not None:
            if num_new_artifacts >= 2:
                self.trigger(message=f"New files detected in {self.resource_path}")


class DataProcessingNode(BaseActionNode):
    def __init__(
        self, name, data_store: BaseResourceNode, dataset_store: BaseResourceNode, loggers = None
    ):
        super().__init__(
            name, 
            predecessors=[data_store, dataset_store], 
            loggers=loggers
        )
        self.data_store = data_store
        self.dataset_store = dataset_store
    
    def execute(self, *args, **kwargs):
        # load the new training data
        artifacts_paths = self.data_store.list_artifacts(state="new")
        for artifact_path in artifacts_paths:
            
            data = None

            # simulate data loading
            with self.data_store.load_artifact(filepath=artifact_path) as fullpath:
                with open(fullpath, "r", encoding="utf-8") as f:
                    data = f.read()
                    self.log(f"Reading Data: {data}", level="DEBUG")
            
            # simulate data processing
            with self.dataset_store.save_artifact(filepath=artifact_path) as fullpath:
                with open(fullpath, "w", encoding="utf-8") as f:
                    processed_data = data.upper()
                    f.write(processed_data)
                    self.log(f"Processed Data: {processed_data}", level="DEBUG")

        return True



metadata_store = SQLiteMetadataStoreNode(name="metadata_store", uri=f"sqlite:///{metadata_store_path}/metadata.db")
data_store = MonitoringDataStoreNode(name="data_store", resource_path=data_store_path, metadata_store=metadata_store)
dataset_store = DatasetStoreNode(name="dataset_store", resource_path=dataset_store_path, metadata_store=metadata_store)
data_processing_node = DataProcessingNode(name="data_processing", data_store=data_store, dataset_store=dataset_store)

# Create the pipeline
pipeline = Pipeline(
    name="test_pipeline", 
    nodes=[metadata_store, data_store, dataset_store, data_processing_node],
    loggers=logger
)

# Create the web server
service = PipelineServer(name="test_pipeline", pipeline=pipeline, host="127.0.0.1", port=8000, logger=logger)

config = service.get_config()
server = AnacostiaServer(config=config)

with server.run_in_thread():
    while True:
        try:
            pass    # Keep the server running
        except (KeyboardInterrupt, SystemExit):
            print("Shutting down the server...")
            break
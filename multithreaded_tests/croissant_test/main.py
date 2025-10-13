import os
import shutil
import logging

from anacostia_pipeline.nodes.metadata.sql.sqlite.node import SQLiteMetadataStoreNode
from anacostia_pipeline.nodes.resources.filesystem.croissant.node import CroissantDataStoreNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.pipelines.pipeline import Pipeline
from anacostia_pipeline.pipelines.server import PipelineServer, AnacostiaServer



# Create the testing artifacts directory for the SQLAlchemy tests
tests_path = "./testing_artifacts"
if os.path.exists(tests_path) is True:
    shutil.rmtree(tests_path)
os.makedirs(tests_path)
metadata_store_path = f"{tests_path}/metadata_store"
data_store_path = f"{tests_path}/data_store"

log_path = f"{tests_path}/anacostia.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='a'
)
logger = logging.getLogger(__name__)



class CroissantTestDataStoreNode(CroissantDataStoreNode):
    def __init__(
        self, 
        name, 
        resource_path, 
        metadata_store = None, 
        metadata_store_client = None, 
        hash_chunk_size = 1048576, 
        max_old_samples = None, 
        remote_predecessors = None, 
        remote_successors = None, 
        client_url = None, 
        wait_for_connection = False, 
        loggers = None, 
        monitoring = True
    ):
        super().__init__(
            name, 
            resource_path, 
            metadata_store, 
            metadata_store_client, 
            hash_chunk_size, 
            max_old_samples, 
            remote_predecessors, 
            remote_successors, 
            client_url, 
            wait_for_connection, 
            loggers, 
            monitoring
        )
    
    def save_data_card(self, files_used):
        self.log(f"Data card saved with files: {files_used}", level="INFO")
    

class DataProcessingNode(BaseActionNode):
    def __init__(
        self, name, data_store: CroissantDataStoreNode, loggers = None
    ):
        super().__init__(
            name, 
            predecessors=[data_store], 
            loggers=loggers
        )
        self.data_store = data_store
    
    def execute(self, *args, **kwargs):
        # load the new training data
        artifacts_paths = self.data_store.list_artifacts(state="new")
        for artifact_path in artifacts_paths:
            
            # loading the data will automatically log the artifact as "current" in the metadata store
            with self.data_store.load_artifact(filepath=artifact_path) as fullpath:
                with open(fullpath, "r", encoding="utf-8") as f:
                    data = f.read()
                    self.log(f"Current Data: {data}", level="DEBUG")
            # after exiting the context manager, the artifact is logged as "used" in the metadata store
 
        return True



metadata_store = SQLiteMetadataStoreNode(name="metadata_store", uri=f"sqlite:///{metadata_store_path}/metadata.db")
data_store = CroissantTestDataStoreNode(name="data_store", resource_path=data_store_path, metadata_store=metadata_store)
data_processing_node = DataProcessingNode(name="data_processing", data_store=data_store)

# Create the pipeline
pipeline = Pipeline(
    name="test_pipeline", 
    nodes=[metadata_store, data_store, data_processing_node],
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
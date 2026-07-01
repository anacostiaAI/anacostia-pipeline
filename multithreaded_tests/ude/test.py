import os
import shutil

from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.pipelines.pipeline import Pipeline
from anacostia_pipeline.pipelines.server import AnacostiaServer

from server import UDEPipelineServer
from metadata_store import UDEMetadataStoreNode
from filesystem_store import UDEFilesystemStoreNode


root_path = "/ged-edap-modelsec/test-container-min-5/anacostia"

# Create the testing artifacts directory for the SQLAlchemy tests
tests_path = "./testing_artifacts"
if os.path.exists(tests_path) is True:
    shutil.rmtree(tests_path)
os.makedirs(tests_path)
metadata_store_path = f"{tests_path}/metadata_store"
data_store_path = f"{tests_path}/data_store"



# override the BaseActionNode to create a custom action node that uses the custom GUI.
# This is just a placeholder for the actual implementation
class UDEActionNode(BaseActionNode):
    def __init__(self, name: str, data_store: FilesystemStoreNode, root_path: str) -> None:
        super().__init__(name=name, predecessors=[data_store])
        self.data_store = data_store
        self.root_path = root_path
    
    def execute(self, *args, **kwargs) -> bool:
        print("Logging node beginning execution")

        artifacts_paths = self.data_store.list_artifacts(state="new")
        for artifact_path in artifacts_paths:
            with self.data_store.load_artifact(filepath=artifact_path) as fullpath:
                with open(fullpath, "r", encoding="utf-8") as f:
                    data = f.read()
                    print(f"Reading data: {data}")

        print("Logging node done executing")
        return True



# Create the nodes
metadata_store = UDEMetadataStoreNode(name="metadata_store", uri=f"sqlite:///{metadata_store_path}/metadata.db")
data_store = UDEFilesystemStoreNode(name="data_store", resource_path=data_store_path, metadata_store=metadata_store)
printing_node = UDEActionNode("logging_node", data_store=data_store, root_path=root_path)

# Create the pipeline
pipeline = Pipeline(name="test_pipeline", nodes=[metadata_store, data_store, printing_node])

# Create the web server
service = UDEPipelineServer(name="test_pipeline", pipeline=pipeline, host="127.0.0.1", port=8000, root_path=root_path)

config = service.get_config()
server = AnacostiaServer(config=config)

# Start the server using the command line: uvicorn test:service
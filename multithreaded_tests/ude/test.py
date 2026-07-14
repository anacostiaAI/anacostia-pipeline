import os
import shutil

from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.resources.filesystem.hugging_face.model_registry.repocard_data import ModelCardData, EvalResult
from anacostia_pipeline.nodes.resources.filesystem.hugging_face.model_registry.repocard import ModelCard
from anacostia_pipeline.nodes.resources.filesystem.utils import locked_file
from anacostia_pipeline.pipelines.pipeline import Pipeline
from anacostia_pipeline.pipelines.server import AnacostiaServer

from server import UDEPipelineServer
from metadata_store import UDEMetadataStoreNode
from filesystem_store import UDEFilesystemStoreNode
from model_registry import UDEModelRegistryNode


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
class UDETrainingNode(BaseActionNode):
    def __init__(
        self, name, model_registry: UDEModelRegistryNode, data_store: FilesystemStoreNode, predecessors,
        remote_predecessors=None, remote_successors=None, client_url=None, wait_for_connection=False, loggers=None
    ):
        super().__init__(name, predecessors, remote_predecessors, remote_successors, client_url, wait_for_connection, loggers)
        self.model_registry = model_registry
        self.data_store = data_store

    def execute(self, *args, **kwargs):

        # load the new training data
        artifacts_paths = self.data_store.list_artifacts(state="new")
        for artifact_path in artifacts_paths:
            # loading the data will automatically log the artifact as "current" in the metadata store
            with self.data_store.load_artifact(filepath=artifact_path) as fullpath:
                with open(fullpath, "r", encoding="utf-8") as f:
                    data = f.read()
                    print(f"Current Data: {data}")
            # after exiting the context manager, the artifact is logged as "used" in the metadata store

        run_id = self.get_run_id()
        model_name = f"model{run_id}.txt"
        model_card_name = f"model{run_id}_card.md"

        card_data = ModelCardData(
            language='en', 
            license='mit', 
            library_name='keras',
            eval_results=[
                EvalResult(
                    task_type='image-classification',
                    dataset_type='beans',
                    dataset_name='Beans',
                    metric_type='accuracy',
                    metric_value=0.9,
                ),
            ],
            model_name=model_name
        )
        card = ModelCard.from_template(
            card_data,
            model_id=model_name,
            model_description="this model does this and that",
            developers="Nate Raw",
            repo="https://github.com/huggingface/huggingface_hub",
            template_path="modelcard.md",
        )
        num_artifacts = self.data_store.get_num_artifacts('all')

        # we're using a simple string as a "model" for testing purposes
        model = f"model_{run_id} with {num_artifacts} data artifacts"

        with self.model_registry.save_model(model_path=model_name) as full_model_path:
            with locked_file(full_model_path, 'w') as f:
                f.write(model)

        if num_artifacts % 3 == 0:
            self.model_registry.save_model_card(
                model_path=model_name,
                model_card_path=model_card_name,
                card=card
            )

        return True




# Create the nodes
metadata_store = UDEMetadataStoreNode(name="metadata_store", uri=f"sqlite:///{metadata_store_path}/metadata.db")
data_store = UDEFilesystemStoreNode(name="data_store", resource_path=data_store_path, metadata_store=metadata_store)
model_registry = UDEModelRegistryNode(
    name="model_registry", resource_path=f"{tests_path}/model_registry", metadata_store=metadata_store, monitoring=False
)
training_node = UDETrainingNode("training_node", model_registry=model_registry, data_store=data_store, predecessors=[data_store, model_registry])

# Create the pipeline
pipeline = Pipeline(name="test_pipeline", nodes=[metadata_store, data_store, model_registry, training_node])

# Create the web server
service = UDEPipelineServer(name="test_pipeline", pipeline=pipeline, host="127.0.0.1", port=8000, root_path=root_path)

config = service.get_config()
server = AnacostiaServer(config=config)

# Start the server using the command line: uvicorn test:service
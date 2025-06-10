import os
import shutil
import logging

from anacostia_pipeline.nodes.metadata.sql.sqlite.node import SQLiteMetadataStoreNode
from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.resources.filesystem.utils import locked_file
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.pipelines.pipeline import Pipeline
from anacostia_pipeline.pipelines.server import PipelineServer
from anacostia_pipeline.nodes.resources.filesystem.hugging_face.model_registry.repocard_data import ModelCardData, EvalResult
from anacostia_pipeline.nodes.resources.filesystem.hugging_face.model_registry.repocard import ModelCard
from anacostia_pipeline.nodes.resources.filesystem.hugging_face.model_registry.node import HuggingFaceModelRegistryNode


# Create the testing artifacts directory for the SQLAlchemy tests
tests_path = "./testing_artifacts"
if os.path.exists(tests_path) is True:
    shutil.rmtree(tests_path)
os.makedirs(tests_path)
metadata_store_path = f"{tests_path}/metadata_store"
data_store_path = f"{tests_path}/data_store"
model_registry_path = f"{tests_path}/model_registry"

log_path = f"{tests_path}/anacostia.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='a'
)
logger = logging.getLogger(__name__)


class TrainingNode(BaseActionNode):
    def __init__(
        self, name, model_registry: HuggingFaceModelRegistryNode, data_store: FilesystemStoreNode, predecessors, 
        remote_predecessors = None, remote_successors = None, client_url = None, wait_for_connection = False, loggers = None
    ):
        super().__init__(name, predecessors, remote_predecessors, remote_successors, client_url, wait_for_connection, loggers)
        self.model_registry = model_registry
        self.data_store = data_store
    
    async def execute(self, *args, **kwargs):
        num_artifacts = await self.data_store.get_num_artifacts('all')
        model_name = f"model{num_artifacts}.txt"
        model_card_name = f"model{num_artifacts}_card.md"

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

        def save_model_fn(filepath: str, model: str) -> None:
            with locked_file(filepath, 'w') as f:
                f.write(model)

        await self.model_registry.save_model(
            save_model_fn=save_model_fn,
            model=f"model {num_artifacts}",
            model_path=model_name,
        )

        if num_artifacts % 3 == 0:
            await self.model_registry.save_model_card(
                model_path=model_name,
                model_card_path=model_card_name,
                card=card
            )

        return True


metadata_store = SQLiteMetadataStoreNode(name="metadata_store", uri=f"sqlite:///{metadata_store_path}/metadata.db")
data_store = FilesystemStoreNode(name="data_store", resource_path=data_store_path, metadata_store=metadata_store)
model_registry = HuggingFaceModelRegistryNode("model_registry", resource_path=model_registry_path, metadata_store=metadata_store, monitoring=False)
training_node = TrainingNode("training", model_registry=model_registry, data_store=data_store, predecessors=[data_store])

# Create the pipeline
pipeline = Pipeline(
    name="test_pipeline", 
    nodes=[metadata_store, data_store, model_registry, training_node],
    loggers=logger
)

# Create the web server
webserver = PipelineServer(name="test_pipeline", pipeline=pipeline, host="127.0.0.1", port=8000, logger=logger)
webserver.run()
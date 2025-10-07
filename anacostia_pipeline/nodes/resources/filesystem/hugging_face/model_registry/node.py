from typing import List, Any, Union, Callable
from logging import Logger

from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.metadata.api import BaseMetadataStoreClient
from anacostia_pipeline.nodes.resources.filesystem.hugging_face.model_registry.repocard import ModelCard
from anacostia_pipeline.nodes.resources.filesystem.hugging_face.model_registry.gui import ModelRegistryGUI



class HuggingFaceModelRegistryNode(FilesystemStoreNode):
    def __init__(
        self, 
        name: str, 
        resource_path: str, 
        metadata_store: BaseMetadataStoreNode = None, 
        metadata_store_client: BaseMetadataStoreClient = None, 
        hash_chunk_size: int = 1_048_576, 
        max_old_samples: int = None, 
        remote_predecessors: List[str] = None, 
        remote_successors: List[str] = None, 
        client_url: str = None, 
        wait_for_connection: bool = False, 
        loggers: Union[Logger, List[Logger]] = None, 
        monitoring: bool = True
    ):
        super().__init__(
            name=name, 
            resource_path=resource_path, 
            metadata_store=metadata_store, 
            metadata_store_client=metadata_store_client, 
            hash_chunk_size=hash_chunk_size,
            max_old_samples=max_old_samples, 
            remote_predecessors=remote_predecessors, 
            remote_successors=remote_successors, 
            client_url=client_url, 
            wait_for_connection=wait_for_connection, 
            loggers=loggers, 
            monitoring=monitoring
        )
    
    def save_model_card(self, model_path: str, model_card_path: str, card: ModelCard):
        """
        Save a model card to the filesystem.
        Args:
            model_path (str): The path where the model is saved.
            model_card_path (str): The path where the model card should be saved.
            card (ModelCard): The model card to save.
            *args: Additional arguments to pass to the save function.
            **kwargs: Additional keyword arguments to pass to the save function.
        """

        with self.save_artifact(model_card_path) as full_path:
            card.save(filepath=full_path)

        self.tag_artifact(filepath=model_card_path, model_path=model_path)
        self.tag_artifact(filepath=model_path, model_card_path=model_card_path)

    def save_model(self, model_path: str, *args, **kwargs):
        """
        Save a model to the filesystem.
        Args:
            model_path (str): The path where the model should be saved.
            *args: Additional arguments to pass to the save function.
            **kwargs: Additional keyword arguments to pass to the save function.
        """
        return self.save_artifact(model_path, *args, **kwargs)

    def setup_node_GUI(self, host: str, port: int, ssl_keyfile: str = None, ssl_certfile: str = None, ssl_ca_certs: str = None) -> ModelRegistryGUI:
        self.gui = ModelRegistryGUI(
            node=self, 
            host=host,
            port=port,
            metadata_store=self.metadata_store, 
            metadata_store_client=self.metadata_store_client,
            ssl_keyfile=ssl_keyfile,
            ssl_certfile=ssl_certfile,
            ssl_ca_certs=ssl_ca_certs
        )
        return self.gui

from typing import List, Union
from logging import Logger

from anacostia_pipeline.nodes.resources.filesystem.hugging_face.model_registry.repocard_data import ModelCardData, EvalResult
from anacostia_pipeline.nodes.resources.filesystem.hugging_face.model_registry.repocard import ModelCard
from anacostia_pipeline.nodes.resources.filesystem.hugging_face.model_registry.node import HuggingFaceModelRegistryNode
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.metadata.api import BaseMetadataStoreClient
from anacostia_pipeline.nodes.resources.filesystem.utils import locked_file


class MinhModelRegistryNode(HuggingFaceModelRegistryNode):
    def __init__(
        self, 
        name: str, 
        resource_path: str, 
        metadata_store: BaseMetadataStoreNode = None, 
        metadata_store_client: BaseMetadataStoreClient = None, 
        init_state: str = "new", 
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
            init_state=init_state, 
            max_old_samples=max_old_samples, 
            remote_predecessors=remote_predecessors, 
            remote_successors=remote_successors, 
            client_url=client_url, 
            wait_for_connection=wait_for_connection, 
            loggers=loggers, 
            monitoring=monitoring
        )
    
    async def save_model(self, model: str, model_path: str, *args, **kwargs):
        """
        Save a model to the filesystem as a text file.
        Args:
            model (str): The model to save.
            model_path (str): The path where the model should be saved.
            *args: Additional arguments to pass to the save function.
            **kwargs: Additional keyword arguments to pass to the save function.
        Returns:
            None
        """

        def save_model_fn(filepath: str, model: str) -> None:
            with locked_file(filepath, 'w') as f:
                f.write(model)

        return await super().save_model(save_model_fn=save_model_fn, model=model, model_path=model_path, *args, **kwargs)
    
    async def load_model(self, model_path: str, *args, **kwargs) -> str:
        """
        Load a model from the filesystem.
        Args:
            model_path (str): The path where the model is saved.
            *args: Additional arguments to pass to the load function.
            **kwargs: Additional keyword arguments to pass to the load function.
        Returns:
            str: The loaded model.
        """

        def load_model_fn(filepath: str) -> str:
            with locked_file(filepath, 'r') as f:
                return f.read()
        
        return self.load_artifact(filepath=model_path, load_fn=load_model_fn, *args, **kwargs)
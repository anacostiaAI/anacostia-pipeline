from typing import List, Any, Union, Callable
from logging import Logger

from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.metadata.api import BaseMetadataStoreClient
from anacostia_pipeline.nodes.resources.filesystem.hugging_face.model_registry.repocard import ModelCard
from anacostia_pipeline.nodes.resources.filesystem.hugging_face.model_registry.repocard_data import ModelCardData


def save_model_card(model_card_path: str, card: ModelCard, *args, **kwargs):
    card.save(filepath=model_card_path)
    

class HuggingFaceModelRegistryNode(FilesystemStoreNode):
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
    
    async def save_model(
        self, 
        save_model_fn: Callable[[str, Any], None], 
        model: Any, 
        model_path: str, 
        model_card_path: str = None, 
        card: ModelCard = None, 
        *args, **kwargs
    ):
        await self.save_artifact(filepath=model_path, save_fn=save_model_fn, model=model)

        # save a model card for the model if given
        if model_card_path is not None and card is not None:
            await self.save_artifact(filepath=model_card_path, save_fn=save_model_card, card=card, *args, **kwargs)
        elif model_card_path is not None and card is None:
            raise ValueError("You provided `model_card_path` but did not provide a `ModelCard` instance (`card`).")
        elif model_card_path is None and card is not None:
            raise ValueError("You provided a `ModelCard` instance (`card`) but did not provide `model_card_path` to save it.")


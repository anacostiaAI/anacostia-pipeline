from typing import List, Any, Union, Callable
from logging import Logger

from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.metadata.api import BaseMetadataStoreClient
from anacostia_pipeline.nodes.resources.filesystem.hugging_face.model_registry.repocard import ModelCard
from anacostia_pipeline.nodes.resources.filesystem.hugging_face.model_registry.repocard_data import ModelCardData


def save_default_model_card(model_card_path: str, card_data: ModelCardData, *args, **kwargs):
    card = ModelCard.from_template(
        card_data=card_data,
        **kwargs
    )
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
        model_card_path: str, 
        card_data: ModelCardData, 
        save_model_card_fn: Callable[[str, Any], None] = save_default_model_card, 
        *args, **kwargs
    ):
        await self.save_artifact(filepath=model_path, save_fn=save_model_fn, model=model)
        await self.save_artifact(filepath=model_card_path, save_fn=save_model_card_fn, card_data=card_data, *args, **kwargs)

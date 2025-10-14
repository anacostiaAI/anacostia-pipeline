from logging import Logger
from typing import List, Union

from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.metadata.api import BaseMetadataStoreClient



class CroissantDatasetStoreNode(FilesystemStoreNode):
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
            monitoring=False   # disable monitoring for the Croissant data store
        )

        self.files_used = []
    
    def before_run_starts(self):
        self.files_used = []
    
    def after_run_ends(self):
        self.save_data_card()
    
    def mark_used(self, filepath):
        super().mark_used(filepath)
        self.files_used.append(filepath)

    def save_data_card(self):
        """
        Save a data card to the filesystem.
        Args:
            files_used (List[str]): The list of files used during the run.
        """
        # Implement the logic to save the data card
        pass
from logging import Logger
from typing import List, Union
from contextlib import contextmanager

from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.metadata.api import BaseMetadataStoreClient
from anacostia_pipeline.nodes.resources.filesystem.croissant.gui import DatasetRegistryGUI



class DatasetRegistryNode(FilesystemStoreNode):
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
    
    def setup_node_GUI(self, host, port, ssl_keyfile = None, ssl_certfile = None, ssl_ca_certs = None):
        self.gui =  DatasetRegistryGUI(
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

    @contextmanager
    def save_data_card(self, data_card_path: str, datasets_paths: List[str], overwrite: bool = False, atomic: bool = True):
        """
        Save a data card to the filesystem.
        Args:
            data_card_path (str): The path where the data card should be saved.
            datasets_paths (List[str]): The list of dataset paths to be included in the data card.
            overwrite (bool): Whether to overwrite the data card if it already exists. Default is False.
            atomic (bool): Whether to use atomic write operations. Default is True.
        Yields:
            str: The full path for you to save the data card.
        Raises:
            Exception: If there is an error during the save operation.
        
        Example Usage:
            with dataset_registry_node.save_data_card(data_card_path="data_card.json", datasets_paths=["data1.txt", "data2.txt"]) as full_path:
                with open(full_path, 'w') as f:
                    f.write("Your data card content here")
        """

        try:
            with self.save_artifact(filepath=data_card_path, overwrite=overwrite, atomic=atomic) as fullpath:
                yield fullpath
            
            # when we get here, the inner manager has already committed and recorded tag the datasets paths to the data card path
            for dataset_path in datasets_paths:
                self.tag_artifact(filepath=dataset_path, data_card_path=data_card_path)
                self.tag_artifact(filepath=data_card_path, dataset_path=dataset_path)

        except Exception as e:
            self.log(f"Failed to save data card {data_card_path}: {e}", level="ERROR")
            raise e
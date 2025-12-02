from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.resources.filesystem.utils import locked_file
from anacostia_pipeline.nodes.resources.filesystem.api import FilesystemStoreClient



def save_text_file(filepath: str, content: str) -> None:
    with locked_file(filepath, 'w') as f:
        f.write(content)

def load_text_file(filepath: str) -> None:
    with locked_file(filepath, "r") as file:
        return file.read()


class MonitoringDataStoreNode(FilesystemStoreNode):
    def __init__(
        self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, max_old_samples: int = None
    ) -> None:
        super().__init__(name=name, resource_path=resource_path, metadata_store=metadata_store, max_old_samples=max_old_samples)

    # one way to customize loading behavior is to create an alias that points to load_artifact
    def load_data(self, filepath: str, *args, **kwargs):
        return super().load_artifact(filepath)


class ModelRegistryClient(FilesystemStoreClient):
    def __init__(
        self, 
        storage_directory, 
        client_name, 
        client_host="127.0.0.1", 
        client_port=8000, 
        server_url=None, 
        loggers=None, 
        ssl_keyfile = None, 
        ssl_certfile = None, 
        ssl_ca_certs = None, 
        *args, **kwargs
    ) -> None:
        super().__init__(
            storage_directory, 
            client_name, 
            client_host, 
            client_port, 
            server_url, 
            loggers, 
            ssl_keyfile, 
            ssl_certfile, 
            ssl_ca_certs, 
            *args, **kwargs
        )
    
    # we can include the same load_model alias in the client and share the client with users on the leaf side of the network.
    # this ensures consistent behavior between the node and client.
    def load_model(self, filepath: str, *args, **kwargs):
        return super().load_artifact(filepath)


class ModelRegistryNode(FilesystemStoreNode):
    def __init__(self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, client_url: str) -> None:
        super().__init__(name, resource_path, metadata_store, max_old_samples=None, client_url=client_url, monitoring=False)

    # another way to customize saving/loading behavior is to create a custom method that wraps the 
    # save_artifact/load_artifact context managers
    def save_model(self, filepath: str, content: str, *args, **kwargs):
        with super().save_artifact(filepath) as fullpath:
            save_text_file(fullpath, content)

    def load_model(self, filepath: str, *args, **kwargs):
        with super().load_artifact(filepath) as fullpath:
            return load_text_file(fullpath)


class PlotsStoreNode(FilesystemStoreNode):
    def __init__(self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, client_url: str) -> None:
        super().__init__(name, resource_path, metadata_store, max_old_samples=None, client_url=client_url, monitoring=False)
    
    def load_plot(self, filepath: str, *args, **kwargs):
        with super().load_artifact(filepath) as fullpath:
            return load_text_file(fullpath)

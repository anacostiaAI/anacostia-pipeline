from fastapi.routing import APIRoute

from anacostia_pipeline.nodes.resources.filesystem.gui import FilesystemStoreGUI
from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode

from metadata_store import UDEMetadataStoreNode



class UDEFilesystemStoreGUI(FilesystemStoreGUI):
    def __init__(self, node, host: str, port: int, root_path: str, ssl_keyfile: str = None, ssl_certfile: str = None, ssl_ca_certs: str = None, *args, **kwargs):
        super().__init__(node=node, host=host, port=port, ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, ssl_ca_certs=ssl_ca_certs, *args, **kwargs)
        self.root_path = root_path

    def get_home_endpoint(self):
        if "/home" in [route.path for route in self.routes if isinstance(route, APIRoute)]:
            return f"{self.root_path}/{self.get_node_prefix()}/home"
        else:
            return ''


# Override the FilesystemStoreNode to create a custom data store node that uses the custom GUI.
class UDEFilesystemStoreNode(FilesystemStoreNode):
    def __init__(self, name: str, resource_path: str, metadata_store: UDEMetadataStoreNode, root_path: str) -> None:
        super().__init__(name=name, resource_path=resource_path, metadata_store=metadata_store)
        self.root_path = root_path
    
    def setup_node_GUI(self, host: str, port: int, ssl_keyfile: str = None, ssl_certfile: str = None, ssl_ca_certs: str = None):
        self.gui = UDEFilesystemStoreGUI(node=self, host=host, port=port, root_path=self.root_path, ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, ssl_ca_certs=ssl_ca_certs)
        return self.gui

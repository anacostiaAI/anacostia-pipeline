from fastapi.routing import APIRoute

from anacostia_pipeline.nodes.gui import BaseGUI
from anacostia_pipeline.nodes.metadata.sql.gui import SQLMetadataStoreGUI
from anacostia_pipeline.nodes.resources.filesystem.gui import FilesystemStoreGUI



class UDEGUI(BaseGUI):
    def __init__(self, node, host: str, port: int, root_path: str, ssl_keyfile: str = None, ssl_certfile: str = None, ssl_ca_certs: str = None, *args, **kwargs):
        super().__init__(node=node, host=host, port=port, ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, ssl_ca_certs=ssl_ca_certs, *args, **kwargs)
        self.root_path = root_path

    def get_home_endpoint(self):
        if "/home" in [route.path for route in self.routes if isinstance(route, APIRoute)]:
            return f"{self.root_path}/{self.get_node_prefix()}/home"
        else:
            return ''


class UDEMetadataStoreGUI(SQLMetadataStoreGUI):
    def __init__(self, node, host: str, port: int, root_path: str, ssl_keyfile: str = None, ssl_certfile: str = None, ssl_ca_certs: str = None, *args, **kwargs):
        super().__init__(node=node, host=host, port=port, ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, ssl_ca_certs=ssl_ca_certs, *args, **kwargs)
        self.root_path = root_path

    def get_home_endpoint(self):
        if "/home" in [route.path for route in self.routes if isinstance(route, APIRoute)]:
            return f"{self.root_path}/{self.get_node_prefix()}/home"
        else:
            return ''


class UDEFilesystemStoreGUI(FilesystemStoreGUI):
    def __init__(self, node, host: str, port: int, root_path: str, ssl_keyfile: str = None, ssl_certfile: str = None, ssl_ca_certs: str = None, *args, **kwargs):
        super().__init__(node=node, host=host, port=port, ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, ssl_ca_certs=ssl_ca_certs, *args, **kwargs)
        self.root_path = root_path

    def get_home_endpoint(self):
        if "/home" in [route.path for route in self.routes if isinstance(route, APIRoute)]:
            return f"{self.root_path}/{self.get_node_prefix()}/home"
        else:
            return ''
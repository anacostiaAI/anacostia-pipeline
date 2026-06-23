from fastapi.routing import APIRoute

from anacostia_pipeline.nodes.metadata.sql.gui import SQLMetadataStoreGUI
from anacostia_pipeline.nodes.metadata.sql.sqlite.node import SQLiteMetadataStoreNode



class UDEMetadataStoreGUI(SQLMetadataStoreGUI):
    def __init__(self, node, host: str, port: int, root_path: str, ssl_keyfile: str = None, ssl_certfile: str = None, ssl_ca_certs: str = None, *args, **kwargs):
        super().__init__(node=node, host=host, port=port, ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, ssl_ca_certs=ssl_ca_certs, *args, **kwargs)
        self.root_path = root_path

    def get_home_endpoint(self):
        if "/home" in [route.path for route in self.routes if isinstance(route, APIRoute)]:
            return f"{self.root_path}/{self.get_node_prefix()}/home"
        else:
            return ''


class UDEMetadataStoreNode(SQLiteMetadataStoreNode):
    def __init__(self, name: str, uri: str, root_path: str) -> None:
        super().__init__(name=name, uri=uri)
        self.root_path = root_path
    
    def setup_node_GUI(self, host: str, port: int, ssl_keyfile: str = None, ssl_certfile: str = None, ssl_ca_certs: str = None):
        self.gui = UDEMetadataStoreGUI(node=self, host=host, port=port, root_path=self.root_path, ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, ssl_ca_certs=ssl_ca_certs)
        return self.gui
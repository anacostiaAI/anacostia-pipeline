from fastapi.routing import APIRoute
from fastapi.responses import HTMLResponse
from fastapi import Request

from anacostia_pipeline.nodes.metadata.sql.sqlite.node import SQLiteMetadataStoreNode
from anacostia_pipeline.nodes.gui import BaseGUI
from anacostia_pipeline.nodes.metadata.sql.fragments import *

html = str  # Define html as an alias for str for type hinting



class UDEMetadataStoreGUI(BaseGUI):
    def __init__(
        self, node, host: str, port: int, 
        ssl_keyfile: str = None, ssl_certfile: str = None, ssl_ca_certs: str = None,
        *args, **kwargs
    ):
        super().__init__(
            node=node, 
            host=host, port=port, 
            ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, ssl_ca_certs=ssl_ca_certs, 
            *args, **kwargs
        )
        self.node = node
        self.host = host
        self.port = port

        @self.middleware("http")
        async def debug_scope(request: Request, call_next):
            print("root_path:", request.scope["root_path"])
            print("path:", request.scope["path"])
            print("raw_path:", request.scope["raw_path"])
            return await call_next(request)

        @self.get("/home", response_class=HTMLResponse)
        async def endpoint(request: Request):
            template: html = f"""
            <p>Welcome to the UDE Metadata Store GUI for node '{self.node.name}'!</p>
            """
            return template

    def get_node_prefix(self):
        return f"/{self.node.name}/hypermedia"

    def get_home_endpoint(self):
        if "/home" in [route.path for route in self.routes if isinstance(route, APIRoute)]:
            return f"{self.node.name}/hypermedia/home"
        else:
            return ''


class UDEMetadataStoreNode(SQLiteMetadataStoreNode):
    def __init__(self, name: str, uri: str) -> None:
        super().__init__(name=name, uri=uri)
    
    def setup_node_GUI(self, host: str, port: int, ssl_keyfile: str = None, ssl_certfile: str = None, ssl_ca_certs: str = None):
        self.gui = UDEMetadataStoreGUI(
            node=self, 
            host=host, port=port, 
            ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, ssl_ca_certs=ssl_ca_certs
        )
        return self.gui
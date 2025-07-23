from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.routing import APIRoute
import asyncio



class BaseGUI(FastAPI):
    def __init__(
        self, 
        node, 
        host: str, 
        port: int, 
        ssl_keyfile: str = None, 
        ssl_certfile: str = None, 
        ssl_ca_certs: str = None, 
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.node = node
        self.host = host
        self.port = port
        self.scheme = "https" if ssl_ca_certs and ssl_certfile and ssl_keyfile else "http"

        @self.get("/status", response_class=HTMLResponse)
        async def status_endpoint():
            return f'''{repr(self.node.status)}'''
    
    def set_event_loop(self, loop: asyncio.AbstractEventLoop):
        """
        Set the event loop for the GUI.
        This is necessary to ensure that the GUI can run in the same event loop as the node.
        """
        self.loop = loop

    def get_node_prefix(self):
        return f"/{self.node.name}/hypermedia"
    
    def get_gui_url(self):
        return f"{self.scheme}://{self.host}:{self.port}{self.get_node_prefix()}"

    def get_home_endpoint(self):
        if "/home" in [route.path for route in self.routes if isinstance(route, APIRoute)]:
            return f"{self.scheme}://{self.host}:{self.port}{self.get_node_prefix()}/home"
        else:
            return ''
    
    def get_status_endpoint(self):
        return f"{self.scheme}://{self.host}:{self.port}{self.get_node_prefix()}/status"

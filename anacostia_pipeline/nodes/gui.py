from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.routing import APIRoute
import httpx



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

        if ssl_ca_certs and ssl_certfile and ssl_keyfile:
            # If SSL certificates are provided, use them to create the client
            self.scheme = "https"
            self.client = httpx.AsyncClient(verify=ssl_ca_certs, cert=(ssl_certfile, ssl_keyfile))
        else:
            # Otherwise, use HTTP
            self.scheme = "http"
            self.client = httpx.AsyncClient()

        @self.get("/status", response_class=HTMLResponse)
        async def status_endpoint():
            return f'''{repr(self.node.status)}'''

    def get_node_prefix(self):
        return f"/{self.node.name}/hypermedia"
    
    def get_gui_url(self):
        return f"{self.scheme}://{self.host}:{self.port}{self.get_node_prefix()}"

    def get_home_endpoint(self):
        if "/home" in [route.path for route in self.routes if isinstance(route, APIRoute)]:
            return f"{self.get_node_prefix()}/home"
        else:
            return ""
    
    def get_status_endpoint(self):
        return f"{self.get_node_prefix()}/status"
    
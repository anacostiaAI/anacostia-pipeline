from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.routing import APIRoute
import httpx



class BaseGUI(FastAPI):
    def __init__(self, node, host: str, port: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.node = node
        self.host = host
        self.port = port
        self.client = httpx.AsyncClient()

        @self.get("/status", response_class=HTMLResponse)
        async def status_endpoint():
            return f'''{repr(self.node.status)}'''

    def get_node_prefix(self):
        return f"/{self.node.name}/hypermedia"
    
    def get_gui_url(self):
        return f"http://{self.host}:{self.port}{self.get_node_prefix()}"
    
    def get_home_endpoint(self):
        if "/home" in [route.path for route in self.routes if isinstance(route, APIRoute)]:
            return f"{self.get_node_prefix()}/home"
        else:
            return ""
    
    def get_status_endpoint(self):
        return f"{self.get_node_prefix()}/status"
    
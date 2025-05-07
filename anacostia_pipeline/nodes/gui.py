from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import httpx

from anacostia_pipeline.nodes.fragments import default_node_page



class BaseGUI(FastAPI):
    def __init__(self, node, host: str, port: int, use_default_router=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.node = node
        self.host = host
        self.port = port

        self.client = httpx.AsyncClient()

        @self.get("/status", response_class=HTMLResponse)
        async def status_endpoint():
            return f'''{repr(self.node.status)}'''
        
        if use_default_router is True:
            @self.get("/home", response_class=HTMLResponse)
            async def endpoint():
                return default_node_page()

    def get_node_prefix(self):
        return f"/{self.node.name}/hypermedia"
    
    def get_gui_url(self):
        return f"http://{self.host}:{self.port}{self.get_node_prefix()}"
    
    def get_endpoint(self):
        return f"{self.get_node_prefix()}/home"
    
    def get_status_endpoint(self):
        return f"{self.get_node_prefix()}/status"
    
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse

from ..components.node_bar import node_bar_closed, node_bar_open, default_node_page, work_template



class BaseNodeApp(FastAPI):
    def __init__(self, node, use_default_router=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.node = node

        @self.get("/status", response_class=HTMLResponse)
        async def status_endpoint(request: Request):
            return f"{repr(self.node.status)}"
        
        @self.get("/work", response_class=HTMLResponse)
        async def work_endpoint(request: Request):
            return work_template(self.node.work_list)
        
        if use_default_router is True:
            @self.get("/home", response_class=HTMLResponse)
            async def endpoint(request: Request):
                return default_node_page(self.get_header_bar_endpoint())

    def get_ip_address(self):
        return "127.0.0.1:8000"
    
    def get_prefix(self):
        return f"/node/{self.node.name}"
    
    def get_header_bar_endpoint(self):
        return f"{self.get_prefix()}/header_bar"

    def get_endpoint(self):
        return f"{self.get_prefix()}/home"
    
    def get_status_endpoint(self):
        return f"{self.get_prefix()}/status"
    
    def get_work_endpoint(self):
        return f"{self.get_prefix()}/work"
    
    def get_edge_endpoint(self, source: str, target: str):
        return f"{self.get_prefix()}/edge/?source={source}&target={target}"
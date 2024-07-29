from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse

from ..components.node_bar import default_node_page, work_template



class BaseNodeApp(FastAPI):
    def __init__(self, node, use_default_router=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.node = node
        self.graph_prefix = None    # graph_prefix variable is set by PipelineWebserver when BaseNodeApp is mounted

        @self.get("/status", response_class=HTMLResponse)
        async def status_endpoint(request: Request):
            return f"{repr(self.node.status)}"
        
        @self.get("/work", response_class=HTMLResponse)
        async def work_endpoint(request: Request):
            return work_template(self.node.work_list)
        
        if use_default_router is True:
            @self.get("/home", response_class=HTMLResponse)
            async def endpoint(request: Request):
                return default_node_page()

    def get_prefix(self):
        return f"/{self.node.name}"
    
    def get_full_prefix(self):
        return f"{self.graph_prefix}{self.get_prefix()}"

    def get_endpoint(self):
        return f"{self.get_prefix()}/home"
    
    def get_status_endpoint(self):
        return f"{self.get_prefix()}/status"
    
    def get_work_endpoint(self):
        return f"{self.get_prefix()}/work"
    
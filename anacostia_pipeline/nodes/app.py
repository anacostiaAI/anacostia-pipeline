from contextlib import asynccontextmanager
from logging import Logger

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
import httpx

from anacostia_pipeline.nodes.fragments import default_node_page, work_template



class BaseApp(FastAPI):
    def __init__(self, node, use_default_router=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.node = node
        self.client = httpx.AsyncClient()

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

    def get_node_prefix(self):
        return f"/{self.node.name}"
    
    def get_endpoint(self):
        return f"{self.get_node_prefix()}/home"
    
    def get_status_endpoint(self):
        return f"{self.get_node_prefix()}/status"
    
    def get_work_endpoint(self):
        return f"{self.get_node_prefix()}/work"
    
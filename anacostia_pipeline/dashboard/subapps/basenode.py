from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse, Response
import asyncio

from anacostia_pipeline.engine.constants import Work
from ..components.node_bar import node_bar_closed, node_bar_open, default_node_page, work_template



class BaseNodeApp(FastAPI):
    def __init__(self, node, header_template: str = None, use_default_router=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.node = node
        self.header_template = header_template

        # Note: the /static directory is not mounted here, but in the main webserver

        @self.get("/status", response_class=HTMLResponse)
        async def status_endpoint(request: Request):
            return f"{repr(self.node.status)}"
        
        @self.get("/work", response_class=HTMLResponse)
        async def work_endpoint(request: Request):
            return work_template(self.node.work_list)
        
        @self.get("/edge/", response_class=HTMLResponse)
        async def edge_endpoint(request: Request, source: str, target: str):
            async def event_stream():
                while True: 
                    if source == self.node.name:
                        if Work.WAITING_SUCCESSORS in self.node.work_list:
                            yield "event: done\n"
                            yield f"data: red\n\n"
                        else:
                            yield "event: done\n" 
                            yield f"data: #333\n\n"

                    await asyncio.sleep(0.2)

            return StreamingResponse(event_stream(), media_type="text/event-stream")
        
        @self.get("/header_bar", response_class=HTMLResponse)
        async def header_bar_endpoint(request: Request, visibility: bool = True):
            if visibility:
                return node_bar_open(
                    node_name = self.node.name, 
                    node_type = self.node.__class__.__name__, 
                    status_endpoint = self.get_status_endpoint(), 
                    work_endpoint = self.get_work_endpoint(), 
                    header_bar_endpoint = f"{self.get_header_bar_endpoint()}?visibility=false"
                )
            else:
                return node_bar_closed(
                    header_bar_endpoint = f"{self.get_header_bar_endpoint()}/?visibility=true"
                )

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
    
    def get_header_template(self):
        return self.header_template
from queue import Queue
from threading import Thread, Event
import time

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import httpx

from anacostia_pipeline.nodes.fragments import default_node_page, work_template
from anacostia_pipeline.utils.constants import Work



class BaseApp(FastAPI):
    def __init__(self, node, use_default_router=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.node = node
        self.client = httpx.AsyncClient()
        self.queue: Queue | None = None
        self.is_running = False
        self.shutdown_event = Event()
        self.edge_color = "black"

        def __monitoring_work():
            while self.shutdown_event.is_set() is False:
                if Work.WAITING_SUCCESSORS in self.node.work_list:
                    if self.edge_color == "black":
                        self.edge_color = "red"
                        for successor in self.node.successors:
                            self.send_edge_color_change_event(self.node.name, successor.name, self.edge_color)
                else:
                    if self.edge_color == "red":
                        self.edge_color = "black"
                        for successor in self.node.successors:
                            self.send_edge_color_change_event(self.node.name, successor.name, self.edge_color)
                
                time.sleep(0.1)
            
        self.work_monitor_thread = Thread(target=__monitoring_work)

        @self.get("/status", response_class=HTMLResponse)
        async def status_endpoint():
            return f"{repr(self.node.status)}"
        
        @self.get("/work", response_class=HTMLResponse)
        async def work_endpoint():
            return work_template(self.node.work_list)
        
        if use_default_router is True:
            @self.get("/home", response_class=HTMLResponse)
            async def endpoint():
                return default_node_page()

    def get_node_prefix(self):
        return f"/{self.node.name}"
    
    def get_endpoint(self):
        return f"{self.get_node_prefix()}/home"
    
    def get_status_endpoint(self):
        return f"{self.get_node_prefix()}/status"
    
    def get_work_endpoint(self):
        return f"{self.get_node_prefix()}/work"
    
    def set_queue(self, queue: Queue):
        self.queue = queue

    def start_monitoring_work(self):
        self.shutdown_event.clear()
        self.work_monitor_thread.start()
    
    def stop_monitoring_work(self):
        self.shutdown_event.set()
        self.work_monitor_thread.join()
    
    def send_edge_color_change_event(self, source: str, target: str, color: str):
        self.queue.put_nowait(
            {
                "event": f"{source}_{target}_change_edge_color",
                "data": color
            }
        )
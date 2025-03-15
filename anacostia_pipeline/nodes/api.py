from fastapi import FastAPI
import httpx



class BaseAPICaller(FastAPI):
    def __init__(self, node, callee_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.node = node
        self.client = httpx.AsyncClient()
        self.callee_name = callee_name
    
    def get_api_prefix(self):
        return f"/{self.node.name}/api"
    
    def get_callee_name(self):
        return self.callee_name



class BaseAPICallee:
    def __init__(self, callee_name: str):
        self.caller_host = None
        self.caller_port = None
        self.caller_name = None
        self.callee_name = callee_name
        self.client = httpx.AsyncClient()
    
    def set_caller(self, sender_host: str, sender_port: int, sender_name: str):
        self.caller_host = sender_host
        self.caller_port = sender_port
        self.caller_name = sender_name
    
    def get_base_url(self):
        return f"http://{self.caller_host}:{self.caller_port}/{self.caller_name}/api"

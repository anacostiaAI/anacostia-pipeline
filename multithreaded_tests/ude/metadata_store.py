from fastapi.routing import APIRoute
from fastapi.responses import HTMLResponse
from fastapi import Request

from anacostia_pipeline.nodes.metadata.sql.sqlite.node import SQLiteMetadataStoreNode
from anacostia_pipeline.nodes.gui import BaseGUI
from anacostia_pipeline.nodes.metadata.sql.fragments import *

from fragments import ude_head_template, ude_sqlmetadatastore_home

html = str  # Define html as an alias for str for type hinting



class UDEMetadataStoreGUI(BaseGUI):
    def __init__(
        self, node, host: str, port: int, 
        ssl_keyfile: str = None, ssl_certfile: str = None, ssl_ca_certs: str = None,
        *args, **kwargs
    ):
        super().__init__(
            node=node, 
            host=host, port=port, 
            ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, ssl_ca_certs=ssl_ca_certs, 
            *args, **kwargs
        )
        self.node = node
        self.host = host
        self.port = port

        self.data_options = {
            "runs": f"{self.get_node_prefix(remove_beginning_slash=True)}/runs",
            "metrics": f"{self.get_node_prefix(remove_beginning_slash=True)}/metrics",
            "params": f"{self.get_node_prefix(remove_beginning_slash=True)}/params",
            "tags": f"{self.get_node_prefix(remove_beginning_slash=True)}/tags",
            "samples": f"{self.get_node_prefix(remove_beginning_slash=True)}/samples",
            "triggers": f"{self.get_node_prefix(remove_beginning_slash=True)}/triggers",
        }

        '''
        @self.middleware("http")
        async def debug_scope(request: Request, call_next):
            print("root_path:", request.scope["root_path"])
            print("path:", request.scope["path"])
            print("raw_path:", request.scope["raw_path"])
            return await call_next(request)
        '''

        @self.get("/home", response_class=HTMLResponse)
        async def _home(request: Request):
            runs = self.node.get_runs()
            for run in runs:
                run['start_time'] = run['start_time'].strftime("%m/%d/%Y, %H:%M:%S")
                if run['end_time'] is not None:
                    run['end_time'] = run['end_time'].strftime("%m/%d/%Y, %H:%M:%S")
            return self.home(runs, request)
        
        @self.get("/runs", response_class=HTMLResponse)
        async def _runs(request: Request):
            runs = self.node.get_runs()
            for run in runs:
                run['start_time'] = run['start_time'].strftime("%m/%d/%Y, %H:%M:%S")
                if run['end_time'] is not None:
                    run['end_time'] = run['end_time'].strftime("%m/%d/%Y, %H:%M:%S")
            return sqlmetadatastore_runs_table(runs, self.data_options["runs"])
        
        @self.get("/samples", response_class=HTMLResponse)
        async def samples(request: Request):
            samples = self.node.get_entries()
            for sample in samples:
                sample['created_at'] = sample['created_at'].strftime("%m/%d/%Y, %H:%M:%S")
                
            return sqlmetadatastore_samples_table(samples, self.data_options["samples"])
        
        @self.get("/metrics", response_class=HTMLResponse)
        async def metrics(request: Request):
            metrics = self.node.get_metrics()
            return sqlmetadatastore_metrics_table(metrics, self.data_options["metrics"])
        
        @self.get("/params", response_class=HTMLResponse)
        async def params(request: Request):
            params = self.node.get_params()
            return sqlmetadatastore_params_table(params, self.data_options["params"])

        @self.get("/tags", response_class=HTMLResponse)
        async def tags(request: Request):
            tags = self.node.get_tags()
            return sqlmetadatastore_tags_table(tags, self.data_options["tags"])
        
        @self.get("/triggers", response_class=HTMLResponse)
        async def triggers(request: Request):
            triggers = self.node.get_triggers()
            for trigger in triggers:
                trigger['trigger_time'] = trigger['trigger_time'].strftime("%m/%d/%Y, %H:%M:%S")
            
            return sqlmetadatastore_triggers_table(triggers, self.data_options["triggers"])

    def home(self, runs, request: Request):
        return ude_sqlmetadatastore_home(
            header_template=ude_head_template(
                '''
                <!-- custom CSS for tables -->
                <link hx-head="re-eval" rel="stylesheet" type="text/css" href="static/css/styles/tables.css">
                <link rel="stylesheet" type="text/css" href="static/css/styles/sqlmetadatastore.css">
                '''
            ), 
            data_options=self.data_options, 
            runs=runs
        )

    def get_node_prefix(self, remove_beginning_slash: bool = False):
        if remove_beginning_slash:
            return f"{self.node.name}/hypermedia"
        else:
            return f"/{self.node.name}/hypermedia"

    def get_home_endpoint(self):
        if "/home" in [route.path for route in self.routes if isinstance(route, APIRoute)]:
            return f"{self.node.name}/hypermedia/home"
        else:
            return ''


class UDEMetadataStoreNode(SQLiteMetadataStoreNode):
    def __init__(self, name: str, uri: str) -> None:
        super().__init__(name=name, uri=uri)
    
    def setup_node_GUI(self, host: str, port: int, ssl_keyfile: str = None, ssl_certfile: str = None, ssl_ca_certs: str = None):
        self.gui = UDEMetadataStoreGUI(
            node=self, 
            host=host, port=port, 
            ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, ssl_ca_certs=ssl_ca_certs
        )
        return self.gui
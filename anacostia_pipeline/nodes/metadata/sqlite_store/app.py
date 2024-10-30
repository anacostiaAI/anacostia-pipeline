from fastapi.responses import HTMLResponse
from fastapi import Request

from anacostia_pipeline.nodes.app import BaseApp
from anacostia_pipeline.nodes.metadata.sqlite_store.fragments import *



class SqliteMetadataStoreApp(BaseApp):
    def __init__(self, node, *args, **kwargs):
        # Create backend server for node by inheriting the BaseNodeApp (i.e., overriding the default router).
        # IMPORTANT: set use_default_router=False to prevent the default /home route from being used
        # after the super().__init__() call inside the constructor
        super().__init__(node, use_default_router=False, *args, **kwargs)

        self.data_options = {
            "runs": f"{self.get_node_prefix()}/runs",
            "metrics": f"{self.get_node_prefix()}/metrics",
            "params": f"{self.get_node_prefix()}/params",
            "tags": f"{self.get_node_prefix()}/tags",
            "samples": f"{self.get_node_prefix()}/samples"
        }

        @self.get("/home", response_class=HTMLResponse)
        async def endpoint(request: Request):
            runs = self.node.get_runs()
            for run in runs:
                run['start_time'] = run['start_time'].strftime("%m/%d/%Y, %H:%M:%S")
                if run['end_time'] is not None:
                    run['end_time'] = run['end_time'].strftime("%m/%d/%Y, %H:%M:%S")
            
            return sqlmetadatastore_home(data_options=self.data_options, runs=runs)
        
        @self.get("/runs", response_class=HTMLResponse)
        async def runs(request: Request):
            runs = self.node.get_runs()
            for run in runs:
                run['start_time'] = run['start_time'].strftime("%m/%d/%Y, %H:%M:%S")
                if run['end_time'] is not None:
                    run['end_time'] = run['end_time'].strftime("%m/%d/%Y, %H:%M:%S")
            
            return sqlmetadatastore_runs_table(runs, self.data_options["runs"])
        
        @self.get("/samples", response_class=HTMLResponse)
        async def samples(request: Request):
            samples = self.node.get_entries(resource_node="all", state="all")
            for sample in samples:
                sample['created_at'] = sample['created_at'].strftime("%m/%d/%Y, %H:%M:%S")
                if sample['end_time'] is not None:
                    sample['end_time'] = sample['end_time'].strftime("%m/%d/%Y, %H:%M:%S")
            
            return sqlmetadatastore_samples_table(samples, self.data_options["samples"])
        
        @self.get("/metrics", response_class=HTMLResponse)
        async def metrics(request: Request):
            rows = self.node.get_metrics(resource_node="all", state="all")
            return sqlmetadatastore_metrics_table(rows, self.data_options["metrics"])
        
        @self.get("/params", response_class=HTMLResponse)
        async def params(request: Request):
            rows = self.node.get_params(resource_node="all", state="all")
            return sqlmetadatastore_params_table(rows, self.data_options["params"])

        @self.get("/tags", response_class=HTMLResponse)
        async def tags(request: Request):
            rows = self.node.get_tags(resource_node="all", state="all")
            return sqlmetadatastore_tags_table(rows, self.data_options["tags"])

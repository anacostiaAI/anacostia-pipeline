from fastapi.responses import HTMLResponse
from fastapi import Request

from anacostia_pipeline.nodes.gui import BaseGUI
from anacostia_pipeline.nodes.metadata.sql.fragments import *



class SQLMetadataStoreGUI(BaseGUI):
    def __init__(self, node, host: str, port: int, *args, **kwargs):
        # Create backend server for node by inheriting the BaseNodeApp (i.e., overriding the default router).
        # IMPORTANT: set use_default_router=False to prevent the default /home route from being used
        # after the super().__init__() call inside the constructor
        super().__init__(node, host, port, *args, **kwargs)

        self.data_options = {
            "runs": f"{self.get_node_prefix()}/runs",
            "metrics": f"{self.get_node_prefix()}/metrics",
            "params": f"{self.get_node_prefix()}/params",
            "tags": f"{self.get_node_prefix()}/tags",
            "samples": f"{self.get_node_prefix()}/samples",
            "triggers": f"{self.get_node_prefix()}/triggers",
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
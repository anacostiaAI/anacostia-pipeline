from fastapi.responses import HTMLResponse
from fastapi import Request

from anacostia_pipeline.nodes.app import BaseApp
from anacostia_pipeline.nodes.metadata.sqlite.fragments import *



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
            samples = self.node.get_entries()
            for sample in samples:
                sample['created_at'] = sample['created_at'].strftime("%m/%d/%Y, %H:%M:%S")
                if sample['end_time'] is not None:
                    sample['end_time'] = sample['end_time'].strftime("%m/%d/%Y, %H:%M:%S")
                
                nodes_info = self.node.get_nodes_info(node_id=sample['node_id'])
                sample['node_name'] = nodes_info[0]['node_name']
            
            return sqlmetadatastore_samples_table(samples, self.data_options["samples"])
        
        @self.get("/metrics", response_class=HTMLResponse)
        async def metrics(request: Request):
            metrics = self.node.get_metrics()
            for metric in metrics:
                nodes_info = self.node.get_nodes_info(node_id=metric['node_id'])
                metric['node_name'] = nodes_info[0]['node_name']

            return sqlmetadatastore_metrics_table(metrics, self.data_options["metrics"])
        
        @self.get("/params", response_class=HTMLResponse)
        async def params(request: Request):
            params = self.node.get_params()
            for param in params:
                nodes_info = self.node.get_nodes_info(node_id=param['node_id'])
                param['node_name'] = nodes_info[0]['node_name']

            return sqlmetadatastore_params_table(params, self.data_options["params"])

        @self.get("/tags", response_class=HTMLResponse)
        async def tags(request: Request):
            tags = self.node.get_tags()
            for tag in tags:
                nodes_info = self.node.get_nodes_info(node_id=tag['node_id'])
                tag['node_name'] = nodes_info[0]['node_name']

            return sqlmetadatastore_tags_table(tags, self.data_options["tags"])

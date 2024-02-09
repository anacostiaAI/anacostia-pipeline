from fastapi.responses import HTMLResponse
from fastapi import Request

from .basenode import BaseNodeApp
from ..components.sqlmetadatastore import sqlmetadatastore_home_template, sqlmetadatastore_runs_template, sqlmetadatastore_samples_template



class SqliteMetadataStoreRouter(BaseNodeApp):
    def __init__(self, node, *args, **kwargs):
        # Create backend server for node by inheriting the BaseNodeApp (i.e., overriding the default router).
        # IMPORTANT: set use_default_router=False to prevent the default /home route from being used
        # IMPORTANT: declare the templates directory, declare the static directory, and declare routes
        # after the super().__init__() call inside the constructor
        super().__init__(
            node, 
            '<link rel="stylesheet" type="text/css" href="/static/css/sqlmetadatastore.css">',
            use_default_router=False, *args, **kwargs
        )

        # Note: the /static directory is not mounted here, but in the main webserver

        self.data_options = {
            "runs": f"{self.get_prefix()}/runs",
            "metrics": f"{self.get_prefix()}/metrics",
            "params": f"{self.get_prefix()}/params",
            "tags": f"{self.get_prefix()}/tags",
            "samples": f"{self.get_prefix()}/samples"
        }

        @self.get("/home", response_class=HTMLResponse)
        async def endpoint(request: Request):
            runs = self.node.get_runs()
            runs = [run.as_dict() for run in runs]
            for run in runs:
                run['start_time'] = run['start_time'].strftime("%m/%d/%Y, %H:%M:%S")
                if run['end_time'] is not None:
                    run['end_time'] = run['end_time'].strftime("%m/%d/%Y, %H:%M:%S")
            
            """
            # IMPORTANT: the context for the TemplateResponse object must include 
            # the request object, the node model, and the status, work, and header bar endpoints;
            # otherwise, the template will not be able to access the information 
            # and by default will respond with the entire page of the DAG
            response = self.templates.TemplateResponse(
                "sqlmetadatastore/sqlmetadatastore.html", 
                {
                    "request": request,
                    "node": self.node.model(), 
                    "status_endpoint": self.get_status_endpoint(),
                    "work_endpoint": self.get_work_endpoint(),
                    "header_bar_endpoint": self.get_header_bar_endpoint(),
                    "data_options": self.data_options,
                    "runs": runs,
                    "runs_endpoint": self.data_options["runs"]
                }
            )
            return response
            """
            return sqlmetadatastore_home_template(
                header_bar_endpoint=self.get_header_bar_endpoint(), data_options=self.data_options, runs=runs
            )
        
        @self.get("/runs", response_class=HTMLResponse)
        async def runs(request: Request):
            runs = self.node.get_runs()
            runs = [run.as_dict() for run in runs]
            for run in runs:
                run['start_time'] = run['start_time'].strftime("%m/%d/%Y, %H:%M:%S")
                if run['end_time'] is not None:
                    run['end_time'] = run['end_time'].strftime("%m/%d/%Y, %H:%M:%S")
            
            """
            response = self.templates.TemplateResponse(
                "sqlmetadatastore/sqlmetadatastore_runs.html", 
                {"request": request, "runs": runs, "runs_endpoint": self.data_options["runs"]}
            )
            return response
            """
            return sqlmetadatastore_runs_template(runs, self.data_options["runs"])
        
        @self.get("/samples", response_class=HTMLResponse)
        async def samples(request: Request):
            samples = self.node.get_entries(resource_node="all", state="all")
            samples = [sample.as_dict() for sample in samples]
            for sample in samples:
                sample['created_at'] = sample['created_at'].strftime("%m/%d/%Y, %H:%M:%S")
                if sample['end_time'] is not None:
                    sample['end_time'] = sample['end_time'].strftime("%m/%d/%Y, %H:%M:%S")
            
            """
            response = self.templates.TemplateResponse(
                "sqlmetadatastore/sqlmetadatastore_samples.html", 
                {"request": request, "samples": samples, "samples_endpoint": self.data_options["samples"]}
            )
            return response
            """
            return sqlmetadatastore_samples_template(samples, self.data_options["samples"])
        
        @self.get("/metrics", response_class=HTMLResponse)
        async def metrics(request: Request):
            rows = self.node.get_metrics(resource_node="all", state="all")
            rows = [sample.as_dict() for sample in rows]

            """
            response = self.templates.TemplateResponse(
                "sqlmetadatastore/sqlmetadatastore_metrics.html", 
                {"request": request, "rows": rows, "metrics_endpoint": self.data_options["metrics"]}
            )
            return response
            """
        
        @self.get("/params", response_class=HTMLResponse)
        async def params(request: Request):
            rows = self.node.get_params(resource_node="all", state="all")
            rows = [sample.as_dict() for sample in rows]

            """
            response = self.templates.TemplateResponse(
                "sqlmetadatastore/sqlmetadatastore_params.html", 
                {"request": request, "rows": rows, "params_endpoint": self.data_options["params"]}
            )
            return response
            """

        @self.get("/tags", response_class=HTMLResponse)
        async def tags(request: Request):
            rows = self.node.get_tags(resource_node="all", state="all")
            rows = [sample.as_dict() for sample in rows]
            
            """
            response = self.templates.TemplateResponse(
                "sqlmetadatastore/sqlmetadatastore_tags.html", 
                {"request": request, "rows": rows, "tags_endpoint": self.data_options["tags"]}
            )
            return response
            """
from anacostia_pipeline.pipelines.server import PipelineServer
from anacostia_pipeline.pipelines.pipeline import Pipeline
from fastapi import Request

from fragments import ude_index_template



class UDEPipelineServer(PipelineServer):
    def __init__(self, name: str, pipeline: Pipeline, host: str, port: int, root_path: str) -> None:
        super().__init__(name=name, pipeline=pipeline, host=host, port=port, root_path=root_path)

    def index(self, request: Request):
        frontend_json = self.frontend_json()
        nodes = frontend_json["nodes"]
        return ude_index_template(nodes, frontend_json, "graph_sse")
        # note: the first line in dag.js had to be changed to:
        # var scriptTag = document.querySelector('script[src="static/js/src/dag.js"]'); 
        # because we are no longer using /static/js/src/dag.js as the path to the dag.js file, 
        # but instead using static/js/src/dag.js as the path to the dag.js file. 
        # This is because we are now using the root_path in the URL, which is /ged-edap-modelsec/test-container-min-5/anacostia, 
        # and the static files are served from /ged-edap-modelsec/test-container-min-5/anacostia/static/js/src/dag.js.

from anacostia_pipeline.pipelines.server import PipelineServer
from anacostia_pipeline.pipelines.pipeline import Pipeline
from fastapi.responses import HTMLResponse
from fastapi import Request

from fragments import ude_index_template, ude_head_template



class UDEPipelineServer(PipelineServer):
    def __init__(self, name: str, pipeline: Pipeline, host: str, port: int, root_path: str) -> None:
        super().__init__(name=name, pipeline=pipeline, host=host, port=port, root_path=root_path)
        self.root_path = root_path

    def index(self, request: Request):
        frontend_json = self.frontend_json()
        nodes = frontend_json["nodes"]
        return ude_index_template(nodes, frontend_json, "/graph_sse", root_path=self.root_path)

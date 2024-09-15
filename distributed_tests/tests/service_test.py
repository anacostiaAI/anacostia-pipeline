from anacostia_pipeline.engine.pipeline import Pipeline, PipelineModel
from anacostia_pipeline.dashboard.subapps.pipeline import RootPipelineWebserver
from anacostia_pipeline.dashboard.subapps.service import AnacostiaService


if __name__ == "__main__":
    service = AnacostiaService(host="localhost", port=8001)
    service.run()
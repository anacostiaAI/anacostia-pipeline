import argparse

from anacostia_pipeline.engine.pipeline import Pipeline, PipelineModel
from anacostia_pipeline.dashboard.subapps.pipeline import PipelineWebserver
from anacostia_pipeline.dashboard.service import AnacostiaService


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('host', type=str)
    parser.add_argument('port', type=int)
    args = parser.parse_args()

    service = AnacostiaService(name="leaf", host=args.host, port=args.port)
    service.run()


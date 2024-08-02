import argparse
import logging

from anacostia_pipeline.engine.pipeline import Pipeline, PipelineModel
from anacostia_pipeline.dashboard.subapps.pipeline import PipelineWebserver
from anacostia_pipeline.dashboard.service import AnacostiaService



root_test_path = "./testing_artifacts"

log_path = f"{root_test_path}/anacostia.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='LEAF %(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='a'
)
logger = logging.getLogger(__name__)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('host', type=str)
    parser.add_argument('port', type=int)
    args = parser.parse_args()

    service = AnacostiaService(name="root", host=args.host, port=args.port)
    service.run()

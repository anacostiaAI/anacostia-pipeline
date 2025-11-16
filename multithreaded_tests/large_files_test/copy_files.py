import os
import shutil
import time
import logging
from logging.config import dictConfig

from loggers import ROOT_ANACOSTIA_LOGGING_CONFIG


prep_artifacts = "./prep_artifacts"
input_store_path = f"{prep_artifacts}/input_store"
tests_path = "./testing_artifacts"
data_store_path = f"{tests_path}/data_store"

dictConfig(ROOT_ANACOSTIA_LOGGING_CONFIG)
logger = logging.getLogger("root_anacostia")


for filepath in os.listdir(input_store_path):
    full_input_path = os.path.join(input_store_path, filepath)
    full_output_path = os.path.join(data_store_path, filepath)
    shutil.copyfile(full_input_path, full_output_path)
    logger.info(f"Copied {full_input_path} -> {full_output_path}")
    time.sleep(2)
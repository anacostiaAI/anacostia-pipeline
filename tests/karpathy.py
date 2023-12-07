from logging import Logger
from typing import List
import unittest
import logging
import sys
import os
import shutil
import random
import time
import traceback

sys.path.append('..')
sys.path.append('../anacostia_pipeline')
from anacostia_pipeline.resources.artifact_store import ArtifactStoreNode
from anacostia_pipeline.resources.metadata_store import JsonMetadataStoreNode
from anacostia_pipeline.engine.base import BaseActionNode, BaseMetadataStoreNode
from anacostia_pipeline.engine.pipeline import Pipeline
from anacostia_pipeline.engine.base import Result
# from anacostia_pipeline.web import app

import torch
import torch.nn as nn
from torch.nn import functional as F


# hyperparameters
batch_size = 64 # how many independent sequences will we process in parallel?
block_size = 256 # what is the maximum context length for predictions?
max_iters = 5000
eval_interval = 500
learning_rate = 3e-4
device = 'cuda' if torch.cuda.is_available() else 'cpu'
eval_iters = 200
n_embd = 384
n_head = 6
n_layer = 6
dropout = 0.2
seed = 1337
# ------------

torch.manual_seed(seed)

print(device)
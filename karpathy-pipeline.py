from logging import Logger
from typing import Any, List
import unittest
import logging
import sys
import os
import math
import shutil
import random
import time
import traceback
import subprocess
import requests
import threading
from dotenv import load_dotenv

from anacostia_pipeline.engine.base import BaseNode, BaseActionNode, BaseMetadataStoreNode
from anacostia_pipeline.engine.pipeline import Pipeline
from anacostia_pipeline.web import Webserver

from anacostia_pipeline.resources.artifact_store import ArtifactStoreNode
from anacostia_pipeline.resources.metadata_store import JsonMetadataStoreNode

import torch
import torch.nn as nn
from torch.nn import functional as F
import plotly.graph_objects as go


# Make sure that the .env file is in the same directory as this Python script
load_dotenv()

# Set the seed for reproducibility
seed_value = 42
random.seed(seed_value)

karpathy_tests_path = "./testing_artifacts/karpathy_tests"
if os.path.exists(karpathy_tests_path) is True:
    shutil.rmtree(karpathy_tests_path)

os.makedirs(karpathy_tests_path)
os.chmod(karpathy_tests_path, 0o777)

log_path = f"{karpathy_tests_path}/anacostia.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='w'
)

# Create a logger
logger = logging.getLogger(__name__)


# hyperparameters
batch_size = 64 # how many independent sequences will we process in parallel?
block_size = 256 # what is the maximum context length for predictions?
max_iters = 2500 #usually 2500
eval_interval = 500
learning_rate = 3e-4
device = 'cuda' if torch.cuda.is_available() else 'cpu'
eval_iters = 50
n_embd = 384
n_head = 6
n_layer = 6
dropout = 0.2
seed = 1337
split_ratio = 0.9     # first 90% will be train, rest val
# ------------

torch.manual_seed(seed)

shakespeare_data_path = "./testing_artifacts/shakespeare"
haiku_data_path = "./testing_artifacts/haiku"

#start with the skakespeare data set to get a character mapping
output_file = f"{shakespeare_data_path}/input.txt"
with open(output_file, 'r', encoding='utf-8') as f:
    text = f.read()

# here are all the unique characters that occur in this text
chars = sorted(list(set(text)))
vocab_size = len(chars)

# create a mapping from characters to integers
stoi = { ch:i for i,ch in enumerate(chars) }
itos = { i:ch for i,ch in enumerate(chars) }
encode = lambda s: [stoi[c] for c in s] # encoder: take a string, output a list of integers
decode = lambda l: ''.join([itos[i] for i in l]) # decoder: take a list of integers, output a string

# Train and test splits
data = torch.tensor(encode(text), dtype=torch.long)
shakespeare_data = data.detach().clone()        # what is the purpose of this line?

# data loading
def get_batch(split, data=data, split_ratio=split_ratio):
    n = int(split_ratio*len(data))
    train_data = data[:n]
    val_data = data[n:]
    
    # generate a small batch of data of inputs x and targets y
    data = train_data if split == 'train' else val_data
    ix = torch.randint(max(0,len(data) - block_size), (batch_size,))
    x = torch.stack([data[i:i+block_size] for i in ix])
    y = torch.stack([data[i+1:i+block_size+1] for i in ix])
    x, y = x.to(device), y.to(device)
    return x, y

@torch.no_grad()
def estimate_loss(model, data=data, split_ratio=split_ratio):
    out = {}
    model.eval()
    for split in ['train', 'val']:
        losses = torch.zeros(eval_iters)
        for k in range(eval_iters):
            # Potenital point of failure here. this function may need data as an argument
            X, Y = get_batch(split, data=data, split_ratio=split_ratio) 
            logits, loss = model(X, Y)
            losses[k] = loss.item()
        out[split] = losses.mean()
    model.train()
    return out

def evaluate_haiku(model, test_data_file="testing_artifacts/haiku/testing.txt"):
    with open(test_data_file, 'r') as f:
        text = f.read()
    test_data = torch.tensor(encode(text), dtype=torch.long)
    losses = estimate_loss(model, data=test_data, split_ratio=0.1)
    return losses['val']

def evaluate_shakespeare(model):
    losses = estimate_loss(model, data=shakespeare_data, split_ratio=0.9)
    return losses['val']

# defining the model structure
class Head(nn.Module):
    """ one head of self-attention """

    def __init__(self, head_size):
        super().__init__()
        self.key = nn.Linear(n_embd, head_size, bias=False)
        self.query = nn.Linear(n_embd, head_size, bias=False)
        self.value = nn.Linear(n_embd, head_size, bias=False)
        self.register_buffer('tril', torch.tril(torch.ones(block_size, block_size)))

        self.dropout = nn.Dropout(dropout)

    def forward(self, x):
        # input of size (batch, time-step, channels)
        # output of size (batch, time-step, head size)
        B,T,C = x.shape
        k = self.key(x)   # (B,T,hs)
        q = self.query(x) # (B,T,hs)
        # compute attention scores ("affinities")
        wei = q @ k.transpose(-2,-1) * k.shape[-1]**-0.5 # (B, T, hs) @ (B, hs, T) -> (B, T, T)
        wei = wei.masked_fill(self.tril[:T, :T] == 0, float('-inf')) # (B, T, T)
        wei = F.softmax(wei, dim=-1) # (B, T, T)
        wei = self.dropout(wei)
        # perform the weighted aggregation of the values
        v = self.value(x) # (B,T,hs)
        out = wei @ v # (B, T, T) @ (B, T, hs) -> (B, T, hs)
        return out

class MultiHeadAttention(nn.Module):
    """ multiple heads of self-attention in parallel """

    def __init__(self, num_heads, head_size):
        super().__init__()
        self.heads = nn.ModuleList([Head(head_size) for _ in range(num_heads)])
        self.proj = nn.Linear(head_size * num_heads, n_embd)
        self.dropout = nn.Dropout(dropout)

    def forward(self, x):
        out = torch.cat([h(x) for h in self.heads], dim=-1)
        out = self.dropout(self.proj(out))
        return out

class FeedFoward(nn.Module):
    """ a simple linear layer followed by a non-linearity """

    def __init__(self, n_embd):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(n_embd, 4 * n_embd),
            nn.ReLU(),
            nn.Linear(4 * n_embd, n_embd),
            nn.Dropout(dropout),
        )

    def forward(self, x):
        return self.net(x)

class Block(nn.Module):
    """ Transformer block: communication followed by computation """

    def __init__(self, n_embd, n_head):
        # n_embd: embedding dimension, n_head: the number of heads we'd like
        super().__init__()
        head_size = n_embd // n_head
        self.sa = MultiHeadAttention(n_head, head_size)
        self.ffwd = FeedFoward(n_embd)
        self.ln1 = nn.LayerNorm(n_embd)
        self.ln2 = nn.LayerNorm(n_embd)

    def forward(self, x):
        x = x + self.sa(self.ln1(x))
        x = x + self.ffwd(self.ln2(x))
        return x

class GPTLanguageModel(nn.Module):

    def __init__(self):
        super().__init__()
        # each token directly reads off the logits for the next token from a lookup table
        self.token_embedding_table = nn.Embedding(vocab_size, n_embd)
        self.position_embedding_table = nn.Embedding(block_size, n_embd)
        self.blocks = nn.Sequential(*[Block(n_embd, n_head=n_head) for _ in range(n_layer)])
        self.ln_f = nn.LayerNorm(n_embd) # final layer norm
        self.lm_head = nn.Linear(n_embd, vocab_size)

        # better init, not covered in the original GPT video, but important, will cover in followup video
        self.apply(self._init_weights)

    def _init_weights(self, module):
        if isinstance(module, nn.Linear):
            torch.nn.init.normal_(module.weight, mean=0.0, std=0.02)
            if module.bias is not None:
                torch.nn.init.zeros_(module.bias)
        elif isinstance(module, nn.Embedding):
            torch.nn.init.normal_(module.weight, mean=0.0, std=0.02)

    def forward(self, idx, targets=None):
        B, T = idx.shape

        # idx and targets are both (B,T) tensor of integers
        tok_emb = self.token_embedding_table(idx) # (B,T,C)
        pos_emb = self.position_embedding_table(torch.arange(T, device=device)) # (T,C)
        x = tok_emb + pos_emb # (B,T,C)
        x = self.blocks(x) # (B,T,C)
        x = self.ln_f(x) # (B,T,C)
        logits = self.lm_head(x) # (B,T,vocab_size)

        if targets is None:
            loss = None
        else:
            B, T, C = logits.shape
            logits = logits.view(B*T, C)
            targets = targets.view(B*T)
            loss = F.cross_entropy(logits, targets)

        return logits, loss

    def generate(self, idx, max_new_tokens):
        # idx is (B, T) array of indices in the current context
        for _ in range(max_new_tokens):
            # crop idx to the last block_size tokens
            idx_cond = idx[:, -block_size:]
            # get the predictions
            logits, loss = self(idx_cond)
            # focus only on the last time step
            logits = logits[:, -1, :] # becomes (B, C)
            # apply softmax to get probabilities
            probs = F.softmax(logits, dim=-1) # (B, C)
            # sample from the distribution
            idx_next = torch.multinomial(probs, num_samples=1) # (B, 1)
            # append sampled index to the running sequence
            idx = torch.cat((idx, idx_next), dim=1) # (B, T+1)
        return idx


model = GPTLanguageModel()
m = model.to(device)

# print the number of parameters in the model
print(sum(p.numel() for p in m.parameters())/1e6, 'M parameters')

# create a PyTorch optimizer
optimizer = torch.optim.AdamW(model.parameters(), lr=learning_rate)

epochs = []
val_loss = []
train_loss = []

# Function to generate and save a line plot
def generate_and_save_line_plot(x: List, y: List, title: str, path: str):
    # Create a Plotly figure with a line plot
    fig = go.Figure(data=go.Scatter(x=x, y=y, mode='lines'))
    fig.update_layout(title=title)

    # Save the plot as an HTML file
    fig.write_html(path)

#generate from the model
def generate_from_model(model, max_new_tokens=5000, output_path = f'{karpathy_tests_path}/more.txt'):
    m = model.to(device)
    context = torch.zeros((1, 1), dtype=torch.long, device=device)
    print(decode(m.generate(context, max_new_tokens=500)[0].tolist()))
    open(output_path, 'w').write(decode(m.generate(context, max_new_tokens=max_new_tokens)[0].tolist()))

def train_model(train_data_file, model: torch.nn.Module, train_plot_path: str, val_plot_path: str, max_iters=max_iters):
    #training_data_file = f"{haiku_data_path}/input.txt"
    with open(train_data_file, 'r', encoding='utf-8') as f:
        text = f.read()
    
    data = torch.tensor(encode(text), dtype=torch.long)
    
    #training loop
    for iter in range(max_iters):
        if iter % 100 == 0:
            print(iter)    

        # every once in a while evaluate the loss on train and val sets
        if iter % eval_interval == 0 or iter == max_iters - 1:
            epochs.append(iter)
            losses = estimate_loss(model, data)
            val_loss.append(losses['val'])
            train_loss.append(losses['train'])
            print(f"step {iter}: train loss {losses['train']:.4f}, val loss {losses['val']:.4f}")
    
        # sample a batch of data
        xb, yb = get_batch('train', data)
    
        # evaluate the loss
        logits, loss = model(xb, yb)
        optimizer.zero_grad(set_to_none=True)
        loss.backward()
        optimizer.step()

    """
    # Create and start a separate thread for plotting
    plot_thread_train = threading.Thread(
        target=generate_and_save_line_plot, args=(epochs, train_loss, "Train Loss", train_plot_path)
    )
    plot_thread_train.daemon = True  # The thread will exit when the main program exits
    plot_thread_train.start()
    
    plot_thread_val = threading.Thread(
        target=generate_and_save_line_plot, args=(epochs, val_loss, "Val Loss", val_plot_path)
    )
    plot_thread_val.daemon = True  # The thread will exit when the main program exits
    plot_thread_val.start()
    """


# ---------------------- defining the pipeline -------------------------------
class MonitoringDataStoreNode(ArtifactStoreNode):
    def __init__(
        self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, 
        init_state: str = "new", max_old_samples: int = None
    ) -> None:
        super().__init__(name, resource_path, metadata_store, init_state, max_old_samples)
    
    def trigger_condition(self) -> bool:
        num_new = self.get_num_artifacts("new")
        return num_new >= 1


class ModelRegistryNode(ArtifactStoreNode):
    def __init__(self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, ) -> None:
        super().__init__(name, resource_path, metadata_store, init_state="new", max_old_samples=None, monitoring=False)
    
    def create_filename(self, filename: str) -> str:
        return f"{filename}_{self.get_num_artifacts('all')}.txt"

    def save_artifact(self, model: torch.nn.Module) -> None:
        filename = self.create_filename()
        filepath = os.path.join(self.path, filename)

        # note: for monitoring-enabled resource nodes, record_artifact should be called before create_file;
        # that way, the Observer can see the file is already logged and ignore it
        self.record_current(filepath)
        torch.save(model.state_dict(), filepath)

    def load_artifact(self, artifact_path: str) -> torch.nn.Module:
        model = GPTLanguageModel()
        model.load_state_dict(torch.load(artifact_path))
        return model


class PlotsStoreNode(ArtifactStoreNode):
    def __init__(self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, ) -> None:
        super().__init__(name, resource_path, metadata_store, init_state="new", max_old_samples=None, monitoring=False)

    def create_filename(self, filename: str) -> str:
        return f"{filename}_{self.get_num_artifacts('all')}"

class ModelRetrainingNode(BaseActionNode):
    def __init__(
        self, name: str, 
        data_store: MonitoringDataStoreNode, plots_store: PlotsStoreNode,
        model_registry: ModelRegistryNode, metadata_store: BaseMetadataStoreNode
    ) -> None:
        self.data_store = data_store
        self.model_registry = model_registry
        self.plots_store = plots_store
        self.metadata_store = metadata_store
        super().__init__(name, predecessors=[data_store, plots_store, model_registry])
    
    def execute(self, *args, **kwargs) -> bool:
        self.log(f"Executing node '{self.name}'")

        old_models = self.model_registry.list_artifacts(state="old")
        if len(old_models) != 0:
            base_model_path = old_models[-1]
        else:
            base_model_path = "./testing_artifacts/shakespeare.pth"

        model = self.model_registry.load_artifact(base_model_path)
        model.to(device)
        
        train_plot_path = self.plots_store.create_filename("train_plot")
        val_plot_path = self.plots_store.create_filename("val_plot")
        for filepath in self.data_store.list_artifacts("current"):
            train_model(filepath, model, train_plot_path, val_plot_path, max_iters=2)
            self.log(f"Trained {base_model_path} on {filepath}")

        self.metadata_store.log_metrics(acc=1.00)
        
        self.metadata_store.log_params(
            batch_size = batch_size, # how many independent sequences will we process in parallel?
            block_size = block_size, # what is the maximum context length for predictions?
            max_iters = max_iters,
            eval_interval = eval_interval,
            learning_rate = learning_rate,
            eval_iters = eval_iters,
            n_embd = n_embd,
            n_head = n_head,
            n_layer = n_layer,
            dropout = dropout,
            seed = seed,
            split_ratio = split_ratio    # first 90% will be train, rest val
        )

        self.metadata_store.set_tags(test_name="Karpathy LLM test")

        self.log(f"Node '{self.name}' executed successfully.")
        return True


class ShakespeareEvalNode(BaseActionNode):
    def __init__(
        self, name: str, predecessors: List[BaseNode], 
        metadata_store: BaseMetadataStoreNode, loggers: Logger | List[Logger] = None
    ) -> None:
        self.metadata_store = metadata_store
        super().__init__(name, predecessors, loggers)
    
    def execute(self, *args, **kwargs) -> bool:
        self.log("Evaluating LLM on Shakespeare validation dataset")
        self.metadata_store.log_metrics(shakespeare_test_loss=1.47)
        return True

class HaikuEvalNode(BaseActionNode):
    def __init__(
        self, name: str, predecessors: List[BaseNode], 
        metadata_store: BaseMetadataStoreNode, loggers: Logger | List[Logger] = None
    ) -> None:
        self.metadata_store = metadata_store
        super().__init__(name, predecessors, loggers)
    
    def execute(self, *args, **kwargs) -> bool:
        self.log("Evaluating LLM on Haiku validation dataset")
        self.metadata_store.log_metrics(haiku_test_loss=2.43)
        return True

class BlockchainNode(BaseActionNode):
    def __init__(self, name: str, metadata_store: JsonMetadataStoreNode, 
        predecessors: List[BaseNode], loggers: Logger | List[Logger] = None
    ) -> None:
        self.metadata_store = metadata_store
        super().__init__(name, predecessors, loggers)
    
    def execute(self, *args, **kwargs) -> bool:
        """
        logic to upload to IPFS
        """

        url = "https://api.quicknode.com/ipfs/rest/v1/s3/put-object"

        tracker_dir = self.metadata_store.tracker_dir
        files_paths = [os.path.join(tracker_dir, json_file_path) for json_file_path in os.listdir(tracker_dir)]

        def send_file(path: str):
            payload = {
                'Key': path,
                'ContentType': 'text'
            }
            files=[
                ('Body', (path, open(path,'rb'),'text/json'))
            ]
            headers = {
                'x-api-key': os.getenv("API_KEY")
            }
            response = requests.request("POST", url, headers=headers, data=payload, files=files)
            self.log(response.text)
        
        for path in files_paths:
            send_file(path)

        return True

path = f"{karpathy_tests_path}"
metadata_store_path = f"{path}/metadata_store"
haiku_data_store_path = f"{path}/haiku"
model_registry_path = f"{path}/model_registry"
plots_path = f"{path}/plots"

metadata_store = JsonMetadataStoreNode(
    name="metadata_store", 
    tracker_dir=metadata_store_path
)
model_registry = ModelRegistryNode(
    "model_registry", 
    model_registry_path, 
    metadata_store
)
plots_store = PlotsStoreNode("plots_store", plots_path, metadata_store)
haiku_data_store = MonitoringDataStoreNode("haiku_data_store", haiku_data_store_path, metadata_store)
retraining = ModelRetrainingNode("retraining 1", haiku_data_store, plots_store, model_registry, metadata_store)
shakespeare_eval = ShakespeareEvalNode("shakespeare_eval", predecessors=[retraining], metadata_store=metadata_store)
haiku_eval = HaikuEvalNode("haiku_eval", predecessors=[retraining], metadata_store=metadata_store)
blockchain = BlockchainNode("blockchain", metadata_store, predecessors=[shakespeare_eval, haiku_eval])
pipeline = Pipeline(
    nodes=[metadata_store, haiku_data_store, model_registry, plots_store, shakespeare_eval, haiku_eval, retraining, blockchain], 
    loggers=logger
)

w = Webserver(pipeline)
w.run()

print('launching nodes')
pipeline.launch_nodes()
time.sleep(2)

haiku_partitions_dir = "./testing_artifacts/haiku"
haiku_files = os.listdir(haiku_partitions_dir)
haiku_files.remove("haiku.csv")
haiku_files.remove("testing.txt")
max_files = 2
for i, filename in enumerate(haiku_files):
    if i < max_files:
        shutil.copy(
            src=os.path.join(haiku_partitions_dir, filename),
            dst=haiku_data_store_path
        )
        time.sleep(3)

time.sleep(180)
pipeline.terminate_nodes()
print('pipeline terminated')

"""
# Train on Shakespeare
train_model(f"{shakespeare_data_path}/input.txt", model, max_iters=2500)

# save model
torch.save(model.state_dict(), "./testing_artifacts/shakespeare.pth")

# load model
model = GPTLanguageModel()
model.load_state_dict(torch.load("./testing_artifacts/shakespeare.pth"))
model.eval()

# generate from Shakespeare
generate_from_model(model)

# train on haiku
# we should have 20 training files. test on 2 for now
haiku_data_path = "./testing_artifacts/haiku"
model = GPTLanguageModel()
model.load_state_dict(torch.load("./testing_artifacts/shakespeare.pth"))
model.to(device)
for i in range(1):
    train_model(f"{haiku_data_path}/training-{i}.txt", model, max_iters=2500)
    torch.save(model.state_dict(), f"./testing_artifacts/haiku-{i}.pth")
    generate_from_model(model)
    print(f"Haiku value = {evaluate_haiku(model)}")
    print(f"Shakespeare value = {evaluate_shakespeare(model)}")
"""
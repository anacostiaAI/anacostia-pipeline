from logging import Logger
from typing import List
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
import threading

sys.path.append('..')
sys.path.append('../anacostia_pipeline')
from anacostia_pipeline.resources.artifact_store import ArtifactStoreNode
from anacostia_pipeline.resources.metadata_store import JsonMetadataStoreNode
from anacostia_pipeline.engine.base import BaseActionNode, BaseMetadataStoreNode
from anacostia_pipeline.engine.pipeline import Pipeline
from anacostia_pipeline.engine.base import Result
import plotly.graph_objects as go
# from anacostia_pipeline.web import app

import torch
import torch.nn as nn
from torch.nn import functional as F


# Set the seed for reproducibility
seed_value = 42
random.seed(seed_value)

karpathy_tests_path = "./testing_artifacts/karpathy_tests"
if os.path.exists(karpathy_tests_path) is True:
    shutil.rmtree(karpathy_tests_path)

os.makedirs(karpathy_tests_path)
os.chmod(karpathy_tests_path, 0o777)

#shakepeare data
shakespeare_data_path = "./testing_artifacts/shakespeare"
url = "https://raw.githubusercontent.com/karpathy/char-rnn/master/data/tinyshakespeare/input.txt"
output_file = f"{shakespeare_data_path}/input.txt"
if os.path.exists(shakespeare_data_path) is False:
    os.makedirs(shakespeare_data_path)
    wget_command = f"wget {url} -O {output_file}"

    try:
        subprocess.run(wget_command, shell=True, check=True)
        print("Download completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error downloading file: {e}")

#haiku data
haiku_data_path = "./testing_artifacts/haiku"
#for now manually insert data
#url = "https://raw.githubusercontent.com/karpathy/char-rnn/master/data/tinyshakespeare/input.txt"
#output_file = f"{shakespeare_data_path}/input.txt"
if os.path.exists(haiku_data_path) is False:
    os.makedirs(haiku_data_path)
    
    """
    wget_command = f"wget {url} -O {output_file}"

    try:
        subprocess.run(wget_command, shell=True, check=True)
        print("Download completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error downloading file: {e}")
    """

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
eval_iters = 200
n_embd = 384
n_head = 6
n_layer = 6
dropout = 0.2
seed = 1337
split_ratio = 0.9     # first 90% will be train, rest val
# ------------

torch.manual_seed(seed)

#start with the skakespeare data set to get a character mapping
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
shakespeare_data = data.detach().clone()

# data loading
def get_batch(split, data=data,split_ratio=split_ratio):
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
            X, Y = get_batch(split, data=data, split_ratio=split_ratio) #Potenital point of failure here. this function may need data as an argument
            logits, loss = model(X, Y)
            losses[k] = loss.item()
        out[split] = losses.mean()
    model.train()
    return out

def evaluate_haiku(model, test_data_file="testing_artifacts/haiku/test.txt"):
    with open(test_data_file, 'r') as f:
        text = f.read()
    test_data = torch.tensor(encode(text), dtype=torch.long)
    losses = estimate_loss(model, data=test_data, split_ratio=0.1)
    return losses['val']

def evaluate_shakespeare(model):
    losses = estimate_loss(model, data=shakespeare_data, split_ratio=0.9)
    return losses['val']

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

epochs = [epoch for epoch in range(max_iters) if epoch % eval_interval == 0]
val_loss = []
train_loss = []

# Function to generate and save a line plot
def generate_and_save_line_plot(x: List, y: List, title: str, path: str):
    try:
        # Create a Plotly figure with a line plot
        fig = go.Figure(data=go.Scatter(x=x, y=y, mode='lines'))
        fig.update_layout(title=title)

        # Save the plot as an HTML file
        fig.write_html(path)

    except Exception as e:
        print(f"Error: {e}")

#generate from the model
def generate_from_model(model, output_path = f'{karpathy_tests_path}/more.txt'):
    m = model.to(device)
    context = torch.zeros((1, 1), dtype=torch.long, device=device)
    print(decode(m.generate(context, max_new_tokens=500)[0].tolist()))
    # open(output_path, 'w').write(decode(m.generate(context, max_new_tokens=10000)[0].tolist()))

def train_model(train_data_file, model, max_iters=max_iters):
    #training_data_file = f"{haiku_data_path}/input.txt"
    with open(train_data_file, 'r', encoding='utf-8') as f:
        text = f.read()
    
    # Train and test splits
    data = torch.tensor(encode(text), dtype=torch.long)
    n = int(split_ratio*len(data))
    train_data = data[:n]
    val_data = data[n:]
    
    #training loop
    for iter in range(max_iters):
        if iter % 100 == 0:
            print(iter)    
        # every once in a while evaluate the loss on train and val sets
        if iter % eval_interval == 0 or iter == max_iters - 1:
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

    # Create and start a separate thread for plotting
    plot_thread_train = threading.Thread(
        target=generate_and_save_line_plot, args=(epochs, train_loss, "Train Loss", f"{karpathy_tests_path}/train_plot.html")
    )
    plot_thread_train.daemon = True  # The thread will exit when the main program exits
    plot_thread_train.start()
    
    plot_thread_val = threading.Thread(
        target=generate_and_save_line_plot, args=(epochs, val_loss, "Val Loss", f"{karpathy_tests_path}/val_plot.html")
    )
    plot_thread_val.daemon = True  # The thread will exit when the main program exits
    plot_thread_val.start()

"""
returns a float in the interval (-1,1)
0 roughly means no net improvement
positive is net improvement, converse for negative

for now this is pseudocode
We would need to be able to access past performance values
this should be stored in some metadata file

We would need to store the negative log likelyhood
"""
# def score_model(model):
#     #shakespeare_old = get_old_shakespeare_score(model)
#     #haiku_old = get_old_haiku_score(model) 
#     shakespeare_new = evaluate_shakespeare_score(model)
#     haiku_new = evaluate_haiku(model)
#     increase  = shakespeare_new / shakespeare_old - 1.0
#     increase += haiku_new / haiku_old - 1.0
#     increase /= 2.0
#     return math.tanh(increase)

#Shakespeare
train_model(f"{shakespeare_data_path}/input.txt", model, max_iters=201)
#generate from shake
generate_from_model(model)
print(f"\n\nHaiku value = {evaluate_haiku(model)}\nShakespeare value = {evaluate_shakespeare(model)}\n\n")
#train on haiku
#we should have 20 training files. test on 2 for now
haiku_data_path = "testing_artifacts/haiku"
for i in range(5):
    train_model(f"{haiku_data_path}/training-{i}.txt", model, max_iters=201)
    generate_from_model(model)
    print(f"\n\nHaiku value = {evaluate_haiku(model)}\nShakespeare value = {evaluate_shakespeare(model)}\n\n")

# Simulate other work in the main thread
for i in range(3):
    print(f"Waiting for plots to be rendered... {i}")
    time.sleep(1)

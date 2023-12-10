import os
import subprocess

#ensure the directories exist
haiku_data_path = "testing_artifacts/haiku"
shakespeare_data_path = "testing_artifacts/shakespeare"

haiku_csv = f"{haiku_data_path}/haiku.csv"
shakespeare_data = f"{shakespeare_data_path}/input.txt"

shakespeare_url = "https://raw.githubusercontent.com/karpathy/char-rnn/master/data/tinyshakespeare/input.txt"
haiku_url = "https://raw.githubusercontent.com/docmarionum1/haikurnn/master/input/poems/haikus.csv"


#download haiku dataset
if os.path.exists(haiku_data_path) is False:
    os.makedirs(haiku_data_path)
wget_cmd = f"wget {haiku_url} -O {haiku_csv}"
try:
    subprocess.run(wget_cmd, shell=True, check=True)
    print(f"Completed downloading haiku.csv")
except subprocess.CalledProcessError as e:
    print(f"Error downloading haiku.csv: {e}")

#download shakespeare
if os.path.exists(shakespeare_data_path) is False:
    os.mkdir(shakespeare_data_path)
wget_cmd = f"wget {shakespeare_url} -O {shakespeare_data}"
try:
    subprocess.run(wget_cmd, shell=True, check=True)
    print(f"Completed downloading input.txt")
except subprocess.CalledProcessError as e:
    print(f"Error downloading input.txt: {e}")


#read both downloaded files
#get their alphabets, remove extra chars from haiku

with open(haiku_csv, 'r') as f:
    haiku = f.readlines()[1:]

with open(shakespeare_data) as f:
    shakespeare = f.read()

#grab only the first three columns of haiku
#grabbing haiku columns from CSV...
for i in range(len(haiku)):
    s = ""
    cols = haiku[i].replace(",", ", ").split(",")[:3]
    for line in cols:
        s = s + line
    haiku[i] = s + "\n"

shakespeare_chars = sorted(list(set(shakespeare)))
haiku_chars = set()
for line in haiku:
    haiku_chars = haiku_chars.union(set(line))
haiku_chars = sorted(list(haiku_chars))
forbiddens = sorted(list(set(haiku_chars) - set(shakespeare_chars)))
print(f"Forbidden characters: {forbiddens}")

#eliminate forbidden characters
print("Eliminating forbidden characters...")
for i in range(len(haiku)):
    for forbidden in forbiddens:
        haiku[i] = haiku[i].replace(forbidden, "")

#90:10 split for training and testing
n = int(len(haiku) * 0.9)
training = haiku[:n]
testing  = haiku[n:]
print(f"training lines: {len(training)}")
print(f"testing lines: {len(testing)}")

#write the testing file
with open(f"{haiku_data_path}/testing.txt", 'w') as testing_file:
    for line in testing:
        testing_file.write(line) 

#write the training files
for i in range(20):
    with open(f"{haiku_data_path}/training-{i}.txt", 'w') as file:
        for i in range(len(training)):
            file.write(training[i])
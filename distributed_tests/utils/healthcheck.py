import os
import requests
import argparse



parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('url', type=str)
args = parser.parse_args()


# in the future, pass the url through command line arguments
response = requests.get(url=args.url)

if response.status_code == 200:
    os._exit(status=0)
else:
    os._exit(status=1)
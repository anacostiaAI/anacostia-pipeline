#!/bin/bash

# generate certs
mkdir -p ./certs
mkcert -key-file ./certs/private_leaf.key -cert-file ./certs/certificate_leaf.pem localhost 127.0.0.1
mkcert -key-file ./certs/private_root.key -cert-file ./certs/certificate_root.pem localhost 127.0.0.1

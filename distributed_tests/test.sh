#!/bin/bash

# run the following command in terminal to execute this bash script:
# $ chmod +x greet.sh
# $ ./test.sh

# Function to delete directories if they exist
delete_directory() {
    if [ -d "$1" ]; then
        echo "Deleting $1"
        rm -rf "$1"
    fi
}

create_directory() {
    echo "Creating $1"
    mkdir "$1"
}

echo "Setting up distributed tests"
python ./utils/setup.py
echo "Done."


# Start pipelines
#python ./root-service/service/main.py &
#fastapi dev ./leaf-service/service/main.py &

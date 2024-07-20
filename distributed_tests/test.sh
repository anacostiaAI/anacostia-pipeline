#!/bin/bash

# run the following command in terminal to execute this bash script:
# $ chmod +x greet.sh
# $ ./test.sh

# Determine the operating system
OS_TYPE=$(uname)

# Define IP addresses and ports
IP1="192.168.100.1"
IP2="192.168.100.2"
PORT1="8000"
PORT2="8080"

# Check if the system is Linux or macOS and then define network interfaces
if [ "$OS_TYPE" = "Linux" ]; then
    echo "Running on Linux"
    INTERFACE1="eth0"
    INTERFACE2="eth1"
elif [ "$OS_TYPE" = "Darwin" ]; then
    echo "Running on macOS"
    INTERFACE1="en0"
    INTERFACE2="en1"
else
    echo "Unsupported operating system: $OS_TYPE"
    exit 1
fi


# Configure the interfaces (replace en0 and en1 with actual interfaces if necessary)
sudo ifconfig $INTERFACE1 alias $IP1 netmask 255.255.255.0 up
sudo ifconfig $INTERFACE2 alias $IP2 netmask 255.255.255.0 up

# Set up testing environment
echo "Setting up distributed tests"
python ./utils/setup.py
echo "Done."


# Start pipelines
python ./root-service/service/main.py &
PID1=$!

#python ./leaf-service/service/main.py &
#PID2=$!

# Function to ping an IP address
ping_ip() {
    local ip=$1
    echo "Pinging $ip..."
    if ping -c 4 $ip > /dev/null 2>&1; then
        echo "$ip is reachable."
    else
        echo "$ip is not reachable."
    fi
}

# Ping the IP addresses
ping_ip $IP1
ping_ip $IP2

# Define a function to clean up background processes
cleanup() {
    echo "Stopping FastAPI servers..."
    kill -15 $PID1
    #kill -15 $PID2
    wait $PID1
    #wait $PID2
    echo "Cleanup done."
}

# Set up the trap to call the cleanup function on SIGINT (Ctrl+C)
trap cleanup SIGINT

# Wait for background processes to complete (or until terminated by Ctrl+C)
wait $PID1

# remove an IP address from en0
sudo ifconfig en0 -alias $IP1
# sudo ifconfig en0 -alias $IP2

# Bring the interface down:
sudo ifconfig en0 down
# sudo ifconfig en1 down

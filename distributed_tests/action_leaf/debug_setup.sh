# Determine the operating system
OS_TYPE=$(uname)

# Define IP addresses and ports
IP_ROOT=$1
PORT_ROOT=$2
IP_LEAF=$3
PORT_LEAF=$4

# Check if the system is Linux or macOS and then define network interfaces
if [ "$OS_TYPE" = "Linux" ]; then
    echo "Running on Linux"
    INTERFACE="eth0"
elif [ "$OS_TYPE" = "Darwin" ]; then
    echo "Running on macOS"
    INTERFACE="en0"
else
    echo "Unsupported operating system: $OS_TYPE"
    exit 1
fi

# Configure the interface 
sudo ifconfig $INTERFACE alias $IP_LEAF netmask 255.255.255.0 up
ifconfig $INTERFACE

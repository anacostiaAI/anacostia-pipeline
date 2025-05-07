# Define IP addresses and ports
IP_LEAF=$1
PORT_LEAF=$2

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

# remove an IP address from en0
echo "Removing IP address $IP_LEAF from $INTERFACE..."
sudo ifconfig $INTERFACE -alias $IP_LEAF

# Bring the interface down:
echo "Bringing $INTERFACE down..."
sudo ifconfig $INTERFACE down

echo "Done."
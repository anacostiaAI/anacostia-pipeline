#!/bin/bash

# generate certs
openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout ./certs/private.key -out ./certs/cert.pem -config ./certs/openssl.cnf

# Configuration
ROOT_SCRIPT="root.py"
ROOT_PORT=8000

LEAF_SCRIPT_1="leaf1.py"
LEAF_PORT_1=8001

LEAF_SCRIPT_2="leaf2.py"
LEAF_PORT_2=8002

is_port_available() {
    if command -v nc >/dev/null 2>&1; then
        nc -z localhost $1 >/dev/null 2>&1
        # Invert the result since nc returns 0 when port is in use
        if [ $? -eq 0 ]; then
            return 1  # Port is in use, so not available
        else
            return 0  # Port is not in use, so available
        fi
    elif command -v lsof >/dev/null 2>&1; then
        lsof -i:$1 >/dev/null 2>&1
        if [ $? -eq 0 ]; then
            return 1  # Port is in use
        else
            return 0  # Port is available
        fi
    else
        echo "Warning: Cannot check port availability. Install 'nc' or 'lsof'."
        return 0
    fi
}

# Set up cleanup function to terminate both processes
cleanup() {
    echo -e "\nTerminating client and server processes..."
    
    # Kill the client first (if it exists)
    if [ -n "$ROOT_PID" ] && kill -0 $ROOT_PID 2>/dev/null; then
        echo "Stopping client (PID: $ROOT_PID)..."
        kill -TERM $ROOT_PID 2>/dev/null
        wait $ROOT_PID 2>/dev/null
    fi
    
    # Then kill the server
    if [ -n "$LEAF_PID_1" ] && kill -0 $LEAF_PID_1 2>/dev/null; then
        echo "Stopping server (PID: $LEAF_PID_1)..."
        kill -TERM $LEAF_PID_1 2>/dev/null
        wait $LEAF_PID_1 2>/dev/null
    fi
    
    # Then kill the server
    if [ -n "$LEAF_PID_2" ] && kill -0 $LEAF_PID_2 2>/dev/null; then
        echo "Stopping server (PID: $LEAF_PID_2)..."
        kill -TERM $LEAF_PID_2 2>/dev/null
        wait $LEAF_PID_2 2>/dev/null
    fi
    
    echo "All processes terminated."
    exit 0
}

# Check server port
if ! is_port_available $ROOT_PORT; then
    echo "Error: Port $ROOT_PORT is already in use."
    exit 1
fi

# Check client port
if ! is_port_available $LEAF_PORT_1; then
    echo "Error: Port $LEAF_PORT_1 is already in use."
    exit 1
fi

# Check client port
if ! is_port_available $LEAF_PORT_2; then
    echo "Error: Port $LEAF_PORT_2 is already in use."
    exit 1
fi

# Register the cleanup function for multiple signals
# This ensures cleanup runs when Ctrl+C is pressed (INT)
# or when the script exits for any reason (EXIT)
# or when the script receives a termination signal (TERM)
trap cleanup EXIT INT TERM

# Both ports are available, set up testing environment
echo "Setting up distributed tests"
python setup.py
echo "Done."

echo "Starting leaf1 server on port $LEAF_PORT_1..."
python3 $LEAF_SCRIPT_1 "127.0.0.1" $LEAF_PORT_1 &
LEAF_PID_1=$!

echo "Starting leaf2 server on port $LEAF_PORT_2..."
python3 $LEAF_SCRIPT_2 "127.0.0.1" $LEAF_PORT_2 &
LEAF_PID_2=$!

# Give the leaf servers time to start
sleep 2

# Verify leaf server started successfully
if ! kill -0 $LEAF_PID_1 2>/dev/null; then
    echo "Error: Leaf1 server failed to start. Check ./testing_artifacts/leaf_server_output.log for details."
    exit 1
fi

# Verify leaf server started successfully
if ! kill -0 $LEAF_PID_2 2>/dev/null; then
    echo "Error: Leaf2 server failed to start. Check ./testing_artifacts/leaf_server_output.log for details."
    exit 1
fi

echo "Starting root server on port $ROOT_PORT connecting to server on port $LEAF_PORT_1 and $LEAF_PORT_2 ..."
python3 $ROOT_SCRIPT "127.0.0.1" $ROOT_PORT "127.0.0.1" $LEAF_PORT_1 "127.0.0.1" $LEAF_PORT_2 &
ROOT_PID=$!

# Give the root server time to start
sleep 2

# Verify leaf server started successfully
if ! kill -0 $ROOT_PID 2>/dev/null; then
    echo "Error: root server failed to start. Check ./testing_artifacts/root_server_output.log for details."
    exit 1
fi

echo "leaf1 server (PID: $LEAF_PID_1), leaf2 server (PID: $LEAF_PID_2), and root server (PID: $ROOT_PID) are running."
echo "Press Ctrl+C to stop both processes."

# Create test data
echo "Creating test data..."
python3 create_files.py &
wait $!
echo "Test complete."

# Keep the script running until interrupted
# This allows the user to press Ctrl+C which will trigger the cleanup function
while true; do
    sleep 1
    # Check if either process has terminated unexpectedly
    if ! kill -0 $LEAF_PID_1 2>/dev/null; then
        echo "Leaf process terminated unexpectedly. Check leaf_server_output.log for details."
        break
    fi
    
    if ! kill -0 $LEAF_PID_2 2>/dev/null; then
        echo "Leaf process terminated unexpectedly. Check leaf_server_output.log for details."
        break
    fi
    
    if ! kill -0 $ROOT_PID 2>/dev/null; then
        echo "Root process terminated. Test may have completed."
        break
    fi
done

# The cleanup function will be called automatically due to the trap

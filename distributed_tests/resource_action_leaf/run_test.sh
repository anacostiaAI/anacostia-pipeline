#!/bin/bash

# remember to make file executable with chmod +x run_test.sh

# Configuration
ROOT_SCRIPT="root.py"
LEAF_SCRIPT="leaf.py"
ROOT_PORT=8000
LEAF_PORT=8001

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
    echo -e "\nTerminating successor and predecessor processes..."
    
    # Kill the successor first so we don't get errors in the logs
    if [ -n "$LEAF_PID" ] && kill -0 $LEAF_PID 2>/dev/null; then
        echo "Stopping successor (PID: $LEAF_PID)..."
        kill -TERM $LEAF_PID 2>/dev/null
        wait $LEAF_PID 2>/dev/null
    fi
    
    # Kill the predecessor first (if it exists)
    if [ -n "$ROOT_PID" ] && kill -0 $ROOT_PID 2>/dev/null; then
        echo "Stopping predecessor (PID: $ROOT_PID)..."
        kill -TERM $ROOT_PID 2>/dev/null
        wait $ROOT_PID 2>/dev/null
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
if ! is_port_available $LEAF_PORT; then
    echo "Error: Port $LEAF_PORT is already in use."
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

echo "Starting leaf server on port $LEAF_PORT..."
python3 $LEAF_SCRIPT "127.0.0.1" $LEAF_PORT &
LEAF_PID=$!

# Give the server time to start
sleep 2

# Verify leaf server started successfully
if ! kill -0 $LEAF_PID 2>/dev/null; then
    echo "Error: Leaf server failed to start. Check ./testing_artifacts/leaf_server_output.log for details."
    exit 1
fi

# Create test data
echo "Creating test data 1..."
python3 create_files_1.py &
wait $!
echo "Created test data 1"

echo "Starting root server on port $ROOT_PORT connecting to server on port $LEAF_PORT..."
python3 $ROOT_SCRIPT "127.0.0.1" $ROOT_PORT "127.0.0.1" $LEAF_PORT &
ROOT_PID=$!

# Verify leaf server started successfully
if ! kill -0 $ROOT_PID 2>/dev/null; then
    echo "Error: root server failed to start. Check ./testing_artifacts/root_server_output.log for details."
    exit 1
fi

echo "leaf server (PID: $LEAF_PID) and root server (PID: $ROOT_PID) are running."
echo "Press Ctrl+C to stop both processes."

# Create test data
echo "Creating test data 2..."
python3 create_files_2.py &
wait $!
echo "Created test data 2"

# Keep the script running until interrupted
# This allows the user to press Ctrl+C which will trigger the cleanup function
while true; do
    sleep 1
    # Check if either process has terminated unexpectedly
    if ! kill -0 $LEAF_PID 2>/dev/null; then
        echo "Leaf process terminated unexpectedly. Check leaf_server_output.log for details."
        break
    fi
    if ! kill -0 $ROOT_PID 2>/dev/null; then
        echo "Root process terminated. Test may have completed."
        break
    fi
done

# The cleanup function will be called automatically due to the trap

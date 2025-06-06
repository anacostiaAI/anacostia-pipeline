#!/bin/bash

# remember to make file executable with chmod +x run_test.sh

SCRIPT="test.py"

# Set up cleanup function to terminate both processes
cleanup() {
    echo -e "\nTerminating pipeline..."
    
    # Kill the client first (if it exists)
    if [ -n "$PID" ] && kill -0 $PID 2>/dev/null; then
        echo "Stopping client (PID: $PID)..."
        kill -TERM $PID 2>/dev/null
        wait $PID 2>/dev/null
    fi
    
    # give the server time to terminate
    sleep 1

    echo "Pipeline terminated."
    exit 0
}

# Register the cleanup function for multiple signals
# This ensures cleanup runs when Ctrl+C is pressed (INT)
# or when the script exits for any reason (EXIT)
# or when the script receives a termination signal (TERM)
trap cleanup EXIT INT TERM

echo "Starting root server on port 8000"
python3 $SCRIPT &
PID=$!

# Give the server time to start
sleep 3

# Create test data
echo "Creating test data..."
python3 create_files.py &
wait $!
echo "Test complete."

# Keep the script running until interrupted
# This allows the user to press Ctrl+C which will trigger the cleanup function
while true; do
    sleep 1
    
    if ! kill -0 $PID 2>/dev/null; then
        echo "Root process terminated. Test may have completed."
        break
    fi
done

# The cleanup function will be called automatically due to the trap
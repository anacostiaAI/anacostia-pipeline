from flask import Flask, jsonify, render_template

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/get_dag_structure')
def get_dag_structure():
    # Extract the DAG structure from the Pipeline class
    # For simplicity, let's assume a static structure here
    dag_structure = {
        "nodes": ["node1", "node2", "node3"],
        "edges": [("node1", "node2"), ("node2", "node3")]
    }
    return jsonify(dag_structure)

@app.route('/get_node_status')
def get_node_status():
    # Return the current status of each node
    # For simplicity, let's assume static statuses here
    node_status = {
        "node1": "green",
        "node2": "yellow",
        "node3": "red"
    }
    return jsonify(node_status)

if __name__ == "__main__":
    app.run(debug=True)

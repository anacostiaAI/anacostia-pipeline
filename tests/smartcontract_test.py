from web3 import Web3

# Connect to your Ethereum node
w3 = Web3(Web3.HTTPProvider('https://polygon-mumbai.g.alchemy.com/v2/XSwc6m9vRd5iUwvACsgyvRlW8sl493VQ'))  # Adjust the provider URL

# Ensure connection is successful
assert w3.isConnected(), "Failed to connect to Ethereum node."

# Contract ABI and address
contract_abi = '<Contract_ABI>'
contract_address = '<Deployed_Contract_Address>'

# Create the contract instance
contract = w3.eth.contract(address=contract_address, abi=contract_abi)

# Function to create a new ML job
def create_job(account, model_details):
    tx_hash = contract.functions.createJob(model_details).transact({'from': account})
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
    print(f"Job created with transaction hash: {receipt.transactionHash.hex()}")

# Function to mark a job as completed
def complete_job(account, job_id):
    tx_hash = contract.functions.completeJob(job_id).transact({'from': account})
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
    print(f"Job {job_id} completed with transaction hash: {receipt.transactionHash.hex()}")

# Example usage
if __name__ == "__main__":
    account = w3.eth.accounts[0]  # Use the first account
    create_job(account, "Model XYZ training")
    complete_job(account, 0)  # Assuming job ID is 0

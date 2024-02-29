from anacostia_pipeline.engine.base import BaseNode
from web3 import Web3

class BlockchainNode(BaseNode):
    def __init__(self, name, predecessors, metadata_store, contract_address, contract_abi, web3_provider):
        super().__init__(name, predecessors, metadata_store)
        self.contract_address = contract_address
        self.contract_abi = contract_abi
        self.w3 = Web3(Web3.HTTPProvider(web3_provider))
        self.contract = self.w3.eth.contract(address=self.contract_address, abi=self.contract_abi)

    def create_job(self, account, model_details):
        tx_hash = self.contract.functions.createJob(model_details).transact({'from': account})
        return self.w3.eth.wait_for_transaction_receipt(tx_hash)

    def complete_job(self, account, job_id):
        tx_hash = self.contract.functions.completeJob(job_id).transact({'from': account})
        return self.w3.eth.wait_for_transaction_receipt(tx_hash)

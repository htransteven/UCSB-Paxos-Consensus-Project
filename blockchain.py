import helpers
from helpers import PAYLOAD_DELIMITER

from hashlib import sha256

def print_blockchain(blockchain):
    result = "\n"
    index = 0
    while index < len(blockchain):
        b = blockchain[index]
        result += f"[{index}] {b.operation.operation} {b.operation.key} {b.operation.value}\n"
        index += 1
    return result

# ex. payload put - key - value - hash - nonce
def parse_block_from_payload(payload):
    payload_tokens = payload.split(PAYLOAD_DELIMITER)
    op = Operation(payload_tokens[0], payload_tokens[1], payload_tokens[2])
    block = Block(op, payload_tokens[3], payload_tokens[4])
    return block

def get_file_name(pid):
    return 'blockchain' + '_p' + pid + '.csv'

# stores array of blocks in csv format
def persist(pid, blockchain):
    file_name = get_file_name(pid)
    with open(file_name, 'w', newline='') as csvfile:
        fieldnames = ['operations', 'prev_hash', 'nonce']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for block in blockchain:
            writer.writerow(block.to_csv())

# returns array of blocks
def reconstruct(pid):
    file_name = get_file_name(pid)
    blockchain = []
    with open(file_name, newline='') as csvfile:
        blocks = csv.reader(csvfile, delimiter=',', quotechar='|')
        firstBlock = True
        for block in blocks:
            # ignore header row
            if firstBlock:
                firstBlock = False
                continue
            operationTokens = (block[0])[1:-1].split(" ")
            operation = Operation(operationTokens[0], operationTokens[1], operationTokens[2])
            blockchain.append(Block(operation, block[1], block[2]))
            # print(f'Added block: {blockchain[-1]}', flush=True)
    # print(f'Reconstructed blockchain: {blockchain}', flush=True)
    return blockchain

def is_valid_nonce(char):
    if(char == "1" or char == "0" or char == "2"):
        return True
    else:
        return False

# str(Block) => <put,someKey,someValue> someReallyLongHash1283812312 35
class Block:
    def __init__(self, op, prev_block = None, nonce = None):
        self.operation = op
        if prev_block != None:
            self.prev_hash = sha256(str(prev_block).encode('utf-8')).hexdigest()
        else:
            self.prev_hash = None
        self.nonce = nonce

    def mine(self):
        randomNonce = 0
        currHash = sha256(str.encode(str(self.operation) + str(randomNonce))).hexdigest()
        while (not is_valid_nonce(currHash[-1])):
            randomNonce += 1
            currHash = sha256(str.encode(str(self.operation) + str(randomNonce))).hexdigest()
        self.nonce = randomNonce
        print(f'Done mining, took {randomNonce} attempts.')

    def to_csv(self):
        return {'operations': str(self.operation), 'prev_hash': str(self.prev_hash), 'nonce': str(self.nonce)}

    def __str__(self):
        return str(self.operation) + helpers.PAYLOAD_DELIMITER + str(self.prev_hash) + helpers.PAYLOAD_DELIMITER + str(self.nonce)

# str(Operation) => <put,someKey,someValue>
class Operation:
    def __init__(self, op, key, value):
        self.operation = op
        self.key = key
        self.value = value
        
    def __str__(self):
     return str(self.operation) + helpers.PAYLOAD_DELIMITER + str(self.key) + helpers.PAYLOAD_DELIMITER + str(self.value)

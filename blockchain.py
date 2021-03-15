import helpers
from helpers import PAYLOAD_DELIMITER

import csv
import os
from hashlib import sha256

def print_blockchain(blockchain):
    result = ""
    index = 0
    while index < len(blockchain):
        b = blockchain[index]
        result += f"[{index}] {b.operation.command} {b.operation.key} {b.operation.value}\n"
        index += 1
    return result

# ex. payload put - key - value - hash - nonce
def parse_block_from_payload(payload):
    payload_tokens = payload.split(PAYLOAD_DELIMITER)
    op = Operation(cmd=payload_tokens[0], key=payload_tokens[1], value=payload_tokens[2])
    block = Block(op=op, prev_hash=payload_tokens[3], nonce=payload_tokens[4])
    return block

def get_file_name(pid):
    return f"blockchain_p{pid}.csv"

# stores array of blocks in csv format
def persist(pid, blockchain, callback = None):
    file_name = get_file_name(pid)
    with open(file_name, 'w', newline='') as csvfile:
        fieldnames = ['operations', 'prev_hash', 'nonce']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # writer.writeheader()
        for block in blockchain:
            writer.writerow(block.to_csv())
    
    if callback:
        callback()

# returns array of blocks
def reconstruct(pid):
    file_name = get_file_name(pid)
    blockchain = []
    with open(file_name, newline='') as csvfile:
        blocks = csv.reader(csvfile, delimiter=',', quotechar='|')
        firstBlock = True
        for block in blocks:
            operationTokens = block[0].split(PAYLOAD_DELIMITER)
            operation = Operation(cmd=operationTokens[0], key=operationTokens[1], value=operationTokens[2])
            blockchain.append(Block(op=operation, prev_hash=block[1], nonce=block[2]))
    return blockchain

def is_valid_nonce(char):
    if(char == "1" or char == "0" or char == "2"):
        return True
    else:
        return False

def hash_block(block):
    return sha256(str(block).encode('utf-8')).hexdigest()

# str(Block) => <put,someKey,someValue> someReallyLongHash1283812312 35
class Block:
    def __init__(self, op, prev_block = None, prev_hash = None, nonce = None):
        self.operation = op
        if prev_hash != None and prev_hash != "None":
            self.prev_hash = prev_hash
        elif prev_block != None and prev_block != "None":
            self.prev_hash = hash_block(prev_block)
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
        # print(f'Done mining, took {randomNonce} attempts.')

    def to_csv(self):
        return {'operations': str(self.operation), 'prev_hash': str(self.prev_hash), 'nonce': str(self.nonce)}

    def __str__(self):
        return str(self.operation) + helpers.PAYLOAD_DELIMITER + str(self.prev_hash) + helpers.PAYLOAD_DELIMITER + str(self.nonce)

# str(Operation) => <put,someKey,someValue>
class Operation:
    def __init__(self, cmd, key, value):
        self.command = cmd
        self.key = key
        self.value = value

    def to_payload(self):
        return f"{self.command}{PAYLOAD_DELIMITER}{self.key}{PAYLOAD_DELIMITER}{self.value}"

    def __eq__(self, other):
        if other == None:
            return False
        return self.command == other.command and self.key == other.key and self.value == other.value
        
    def __str__(self):
     return str(self.command) + helpers.PAYLOAD_DELIMITER + str(self.key) + helpers.PAYLOAD_DELIMITER + str(self.value)

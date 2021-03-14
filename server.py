import helpers
from helpers import PAYLOAD_DELIMITER
import blockchain as bc

import socket
import csv
import threading
import sys
import queue
import time

# Socket Information
IP = socket.gethostname()
encoding = 'utf-8'
port_base = 6000
pid = int(sys.argv[1])  # pid = 1, 2, 3, 4, or 5
max_clients = 4

broken_streams = {
    1: False,
    2: False,
    3: False,
    4: False,
    5: False
}
broken_streams_lock = threading.Lock()

def update_broken_streams(server_id, status):
    global broken_streams, broken_streams_lock
    broken_streams_lock.acquire()
    broken_streams[server_id] = status
    broken_streams_lock.release()

# Server data
temporary_operations = queue.Queue(maxsize=0)
database = {}
blockchain = []
blockchain_lock = threading.Lock()

def set_blockchain(chain):
    global blockchain, blockchain_lock
    blockchain_lock.acquire()
    blockchain = chain
    blockchain_lock.release()

def append_to_blockchain(block):
    global database, blockchain, blockchain_lock
    blockchain_lock.acquire()

    print(f"Appending block: {block.operation}")
    if block.operation.command == "put":
        database[block.operation.key] = block.operation.value

    blockchain.append(block)
    blockchain_lock.release()

# Paxos Information
acks = 0
acks_lock = threading.Lock()
highest_received_ballot = (0,0)
highest_received_ballot_lock = threading.Lock()
highest_received_val = None
highest_received_val_lock = threading.Lock()

def increment_acks():
    global acks, acks_lock
    acks_lock.acquire()
    acks += 1
    acks_lock.release()

def reset_acks():
    global acks, acks_lock
    acks_lock.acquire()
    acks = 0
    acks_lock.release()

def set_highest_received_ballot(ballot):
    global highest_received_ballot, highest_received_ballot_lock
    highest_received_ballot_lock.acquire()
    highest_received_ballot = ballot
    highest_received_ballot_lock.release()

def reset_highest_received_ballot():
    set_highest_received_ballot((0,0))

def set_highest_received_val(val):
    global highest_received_val, highest_received_val_lock
    highest_received_val_lock.acquire()
    highest_received_val = val
    highest_received_val_lock.release()

def reset_highest_received_val():
    set_highest_received_val(None)

ballot_num = (0, pid)
accept_num = (0, pid)
accept_val = None
ballot_num_lock = threading.Lock()
accept_num_lock = threading.Lock()
accept_val_lock = threading.Lock()

def increment_ballot_num():
    global ballot_num, ballot_num_lock
    ballot_num_lock.acquire()
    ballot_num = (ballot_num[0] + 1, pid)
    ballot_num_lock.release()

def set_ballot_num(num):
    global ballot_num, ballot_num_lock
    ballot_num_lock.acquire()
    ballot_num = num
    ballot_num_lock.release()

def set_accept_num(num):
    global accept_num, accept_num_lock
    accept_num_lock.acquire()
    accept_num = num
    accept_num_lock.release()

def reset_accept_num():
    set_accept_num((0,pid))

def set_accept_val(val):
    global accept_val, accept_val_lock
    accept_val_lock.acquire()
    accept_val = val
    accept_val_lock.release()

def reset_accept_val():
    set_accept_val(None)

# Outgoing socket 1
sock_out1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_out1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

# Outgoing socket 2
sock_out2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_out2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

# Incoming socket handles ALL incoming connection requests
sock_in1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_in1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock_in1.bind((IP, port_base + pid))
sock_in1.listen(20)
inputStreams = []

#######################################################################
# FUNCTIONS TO SEND MESSAGES
#######################################################################
def direct_message(message, stream):
    helpers.broadcast_message(f"server {pid} -> {message}", [stream])

# could include processor id in message if we needed
# general broadcasting function prepends [pid] before all messages
def broadcast_message(message):
    global sock_out1, sock_out2, inputStreams
    helpers.broadcast_message(f"server {pid} -> {message}", [sock_out1, sock_out2] + inputStreams)

# ex. prepare - 3,2
def broadcast_prepare():
    global ballot_num
    broadcast_message(f"prepare{PAYLOAD_DELIMITER}{ballot_num[0]},{ballot_num[1]}")

# ex. promise - 3,2 - 2,2 - put 'hello world'
def send_promise(stream):
    global ballot_num, accept_num, accept_val
    direct_message(f"promise{PAYLOAD_DELIMITER}{ballot_num[0]},{ballot_num[1]}{PAYLOAD_DELIMITER}{accept_num[0]},{accept_num[1]}{PAYLOAD_DELIMITER}{accept_val}", stream)

# ex. accept - 3,2 - put 'hello world'
def broadcast_accept():
    global ballot_num, accept_val
    broadcast_message(f"accept{PAYLOAD_DELIMITER}{ballot_num[0]},{ballot_num[1]}{PAYLOAD_DELIMITER}{accept_val}")

# ex. accepted - 3,2 - put 'hello world'
def send_accepted(stream):
    global accept_num, accept_val
    direct_message(f"accepted{PAYLOAD_DELIMITER}{accept_num[0]},{accept_num[1]}{PAYLOAD_DELIMITER}{accept_val}", stream)

# ex. decide - 3,2 - put 'hello world'
def broadcast_decide():
    global ballot_num, accept_val
    broadcast_message(f"decide{PAYLOAD_DELIMITER}{ballot_num[0]},{ballot_num[1]}{PAYLOAD_DELIMITER}{accept_val}")

#######################################################################
# FUNCTIONS TO HANDLE RECEIVED MESSAGES
#######################################################################
def handle_received_failLink(src, dest):
    global pid
    if pid == dest:
        print(f"[{dest}] --// broken stream //-- [{src}]")
        update_broken_streams(src, True)

def handle_received_fixLink(src,dest):
    global pid
    if pid == dest:
        print(f"[{dest}] --- fixed stream ---- [{src}]")
        update_broken_streams(src, False)

# Begin Paxos functions
def handle_received_prepare(received_ballot_num, stream):
    global ballot_num
    if received_ballot_num >= ballot_num:
        set_ballot_num(received_ballot_num)
        send_promise(stream)

def handle_received_promise(received_accept_num, received_accept_val):
    global highest_received_ballot
    if received_accept_num > highest_received_ballot and received_accept_val != "None" and len(received_accept_val) > 0:
        set_highest_received_ballot(received_accept_num)
        set_highest_received_val(received_accept_val)
    
    increment_acks()

def handle_received_accept(received_ballot_num, received_accept_val, stream):
    global ballot_num
    if received_ballot_num >= ballot_num:
        set_ballot_num((received_ballot_num[0] + 1, pid))
        set_accept_num(received_ballot_num)
        block = bc.parse_block_from_payload(received_accept_val)
        set_accept_val(block)
        append_to_blockchain(block)

        send_accepted(stream)

def handle_received_accepted():
    increment_acks()

def handle_received_decide(received_ballot_num, received_accept_val):
    global accept_num
    if (received_ballot_num == accept_num):
        return
    
    set_ballot_num((received_ballot_num[0] + 1, pid))
    set_accept_num(received_ballot_num)

    block = bc.parse_block_from_payload(received_accept_val)
    set_accept_val(block)
    append_to_blockchain(block)
# End Paxos functions

def handle_blockchain_reconstruct():
    global pid, blockchain

    persisted_blockchain = bc.reconstruct(pid)
    for b in persisted_blockchain:
        append_to_blockchain(b)

def handle_operations_queue():
    global temporary_operations

    while True:
        next_op = temporary_operations.get() # defaults to block=True and timeout=None
        threading.Thread(target=begin_paxos,
                                    args=(next_op[0],next_op[1]), daemon=True).start()

def handle_get_callback(stream):
    def callback(block):
        global database
        if block.operation.key in database:
            direct_message(f"{block.operation.key}: {database[block.operation.key]}", stream)
        else:
            direct_message(f"no value was found for the key: {block.operation.key}", stream)
    return callback

def handle_put_callback(stream):
    def callback(block):
        global database
        direct_message(f"{block.operation.key}: {block.operation.value}", stream)
    return callback

def server_communications(stream):
    global pid, blockchain, database, broken_streams
    addr = stream.getsockname()
    while True:
        data = stream.recv(1024)
        if data:
            decoded = data.decode(encoding)
            # print(f'Data received ({decoded}) from {addr}', flush=True)
            # example received message = server -> payload'
            tokens = decoded.split(" -> ")
            sender_tokens = tokens[0].split(" ")
            sender = sender_tokens[0]
            payload = tokens[1]
            if sender == "client":
                print(f'[C]: {payload}', flush=True)
                # example payload = put - student_id1 - '858-123-4567'
                payload_tokens = payload.split(PAYLOAD_DELIMITER)
                command = payload_tokens[0]
                if command == "broadcast":
                    sentence = payload_tokens[1].strip("'")
                    threading.Thread(target=broadcast_message,
                                    args=(sentence,), daemon=True).start()
                elif command == "put":
                    key = payload_tokens[1]
                    value = payload_tokens[2].strip("'")

                    temporary_operations.put((bc.Operation(command, key, value), handle_put_callback(stream)))
                elif command == "get":
                    key = payload_tokens[1]

                    temporary_operations.put((bc.Operation(command, key, None), handle_get_callback(stream)))
                elif command == "persist":
                    threading.Thread(target=bc.persist,
                                    args=(pid, blockchain), daemon=True).start()
                elif command == "reconstruct":
                    threading.Thread(target=handle_blockchain_reconstruct,
                                    args=(), daemon=True).start()
                elif command == "exit":
                    helpers.handle_exit([inputStreams[0], inputStreams[1], sock_out1, sock_out2])
                elif command == "blockchain":
                    print(f"Blockchain: {bc.print_blockchain(blockchain)}")
                elif command == "database":
                    print(f"Database: {database}")
            elif sender == "server":
                sender_pid = int(sender_tokens[1])
                # example payload = promise - 3,2, - 2,2 - put 'hello world'
                payload_tokens = payload.split(PAYLOAD_DELIMITER)
                command = payload_tokens[0]
                if command == "fixLink":
                    src = int(payload_tokens[1])
                    dest = int(payload_tokens[2])
                    threading.Thread(target=handle_received_fixLink,
                            args=(src, dest), daemon=True).start()
                else:
                    if broken_streams[sender_pid] == True:
                        continue

                print(f'[P{sender_pid}]: {payload}', flush=True)
                
                if command == "failLink":
                    src = int(payload_tokens[1])
                    dest = int(payload_tokens[2])
                    threading.Thread(target=handle_received_failLink,
                            args=(src, dest), daemon=True).start()
                elif command == "prepare":
                    received_ballot_num = tuple(int(e) for e in payload_tokens[1].split(","))

                    threading.Thread(target=handle_received_prepare,
                                    args=(received_ballot_num,stream), daemon=True).start()
                elif command == "promise":
                    received_ballot_num = tuple(int(e) for e in payload_tokens[1].split(","))
                    received_accept_num = tuple(int(e) for e in payload_tokens[2].split(","))
                    received_accept_val = PAYLOAD_DELIMITER.join(payload_tokens[3:])

                    threading.Thread(target=handle_received_promise,
                                    args=(received_accept_num,received_accept_val), daemon=True).start()
                elif command == "accept":
                    received_ballot_num = tuple(int(e) for e in payload_tokens[1].split(","))
                    received_accept_val = PAYLOAD_DELIMITER.join(payload_tokens[2:])

                    threading.Thread(target=handle_received_accept,
                                    args=(received_ballot_num,received_accept_val, stream), daemon=True).start()
                elif command == "accepted":
                    threading.Thread(target=handle_received_accepted,
                                    args=(), daemon=True).start()
                elif command == "decide":
                    received_ballot_num = tuple(int(e) for e in payload_tokens[1].split(","))
                    received_accept_val = PAYLOAD_DELIMITER.join(payload_tokens[2:])

                    threading.Thread(target=handle_received_decide,
                                    args=(received_ballot_num, received_accept_val), daemon=True).start()
                elif command == "persist":
                    threading.Thread(target=bc.persist,
                                    args=(pid, blockchain), daemon=True).start()
                elif command == "reconstruct":
                    threading.Thread(target=handle_blockchain_reconstruct,
                                    args=(), daemon=True).start()
                elif command == "blockchain":
                    print(f"Blockchain: {bc.print_blockchain(blockchain)}")
                elif command == "database":
                    print(f"Database: {database}")
        else:
            break

#######################################################################
# LEADER ELECTION 
# 1. Server is alive
# 2. If server receives message from client, begin leader election
#######################################################################
def begin_paxos(proposed_operation, callback):
    global acks, ballot_num, accept_val, highest_received_val

    # phase 1
    broadcast_prepare()
    timeout_time = time.time() + 5  # timeout after 5 seconds
    while (acks < max_clients / 2):
        if time.time() >= timeout_time:
            print(f"ballot {ballot_num} timed out during phase 1")
            return

    operation = proposed_operation
    prev_block = None
    if len(blockchain) > 0:
        prev_block = blockchain[-1]
    block = bc.Block(operation, prev_block)
    block.mine()

    set_accept_val(block)
    increment_ballot_num()

    # phase 2
    # ballot recovery
    if highest_received_val != None:
        print(f"recovered {highest_received_val} from previous paxos sequence")
        set_accept_val(bc.parse_block_from_payload(highest_received_val))

    reset_acks()
    broadcast_accept()
    timeout_time = time.time() + 5  # timeout after 5 seconds
    while (acks < max_clients / 2):
        if time.time() >= timeout_time:
            print(f"ballot {ballot_num} timed out during phase 2")
            return
    
    append_to_blockchain(accept_val)

    # phase 3
    broadcast_decide()

    # reset

    callback(accept_val)

# accept any incoming socket connection and create a thread to handle communication on this new stream
def accept_connections():
    while True:
        stream, addr = sock_in1.accept()
        inputStreams.append(stream)
        print(f'Connected to {addr}', flush=True)
        threading.Thread(target=server_communications,
                         args=(stream,), daemon=True).start()

# send out 2 connection requests as dictated by the connection scheme in helpers.py
# keep sending connection requests until a stream is created and then handle the communication of that stream in a new thread
def send_connections():
    out_addr1, out_addr2 = helpers.get_output_connection_tuples(pid, IP, port_base)
    sock_out1_result = None
    sock_out2_result = None
    while (sock_out1_result != 0 or sock_out2_result != 0):
        sock_out1_result = sock_out1.connect_ex(out_addr1)
        sock_out2_result = sock_out2.connect_ex(out_addr2)
        if sock_out1_result == 0:
            threading.Thread(target=server_communications,
                     args=(sock_out1,), daemon=True).start()
        if sock_out2_result == 0:
            threading.Thread(target=server_communications,
                     args=(sock_out2,), daemon=True).start()
        time.sleep(1)

def handle_failLink(src, dest):
    broadcast_message(f"failLink{PAYLOAD_DELIMITER}{src}{PAYLOAD_DELIMITER}{dest}")
    update_broken_streams(dest, True)

def handle_fixLink(src, dest):
    broadcast_message(f"fixLink{PAYLOAD_DELIMITER}{src}{PAYLOAD_DELIMITER}{dest}")
    update_broken_streams(dest, False)

def input_listener():
    global blockchain
    user_input = input()
    while True:
        tokens = user_input.split(" ")
        command = tokens[0]
        if command == "failLink":
            src = int(tokens[1])
            dest = int(tokens[2])
            threading.Thread(target=handle_failLink,
                     args=(src, dest), daemon=True).start()
        elif command == "fixLink":
            src = int(tokens[1])
            dest = int(tokens[2])
            threading.Thread(target=handle_fixLink,
                     args=(src, dest), daemon=True).start()
        elif command == "blockchain":
            print(f"Blockchain: {bc.print_blockchain(blockchain)}")
        user_input = input()

threading.Thread(target=accept_connections, args=()).start()
threading.Thread(target=send_connections, args=()).start()
threading.Thread(target=handle_operations_queue, args=()).start()
threading.Thread(target=input_listener, args=()).start()

while True:
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        helpers.handle_exit([sock_in1, sock_out1, sock_out2])
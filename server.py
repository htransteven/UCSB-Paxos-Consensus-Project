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
MESSAGE_DELAY = 1.5

leader_pid = None
leader_pid_lock = threading.Lock()
leader_stream = None
leader_stream_lock = threading.Lock()
new_election = False
new_election_lock = threading.Lock()

def set_new_election(value):
    global new_election, new_election_lock
    new_election_lock.acquire()
    new_election = value
    new_election_lock.release()

def set_leader_pid(id):
    global leader_pid, leader_pid_lock
    leader_pid_lock.acquire()
    leader_pid = id
    leader_pid_lock.release()

def set_leader_stream(stream):
    global leader_stream, leader_stream_lock
    leader_stream_lock.acquire()
    leader_stream = stream
    leader_stream_lock.release()

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
# temporary_operations = []
# temporary_operations_lock = threading.Lock()

# def append_operation(op):
#     global temporary_operations, temporary_operations_lock
#     temporary_operations_lock.acquire()
#     temporary_operations.append(op)
#     temporary_operations_lock.release()

# def pop_operation():
#     global temporary_operations, temporary_operations_lock
#     temporary_operations_lock.acquire()
#     if len(temporary_operations) > 0:
#         temporary_operations.pop(0)
#     temporary_operations_lock.release()


database = {}
blockchain = []
blockchain_lock = threading.Lock()

def set_blockchain(chain):
    global blockchain, blockchain_lock
    blockchain_lock.acquire()
    blockchain = chain
    blockchain_lock.release()

def reset_blockchain():
    set_blockchain([])

def append_to_blockchain(block):
    global pid, database, blockchain, blockchain_lock
    blockchain_lock.acquire()

    log(f"Appending block: {block.operation}")
    if block.operation.command == "put":
        database[block.operation.key] = block.operation.value

    blockchain.append(block)
    threading.Thread(target=bc.persist,
                                    args=(pid, blockchain), daemon=True).start()
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
    set_accept_num((0,0))

def set_accept_val(val):
    global accept_val, accept_val_lock
    accept_val_lock.acquire()
    accept_val = val
    accept_val_lock.release()

def reset_accept_val():
    set_accept_val(None)

def get_object_string(obj):
    return f"{obj}".replace(",", "\n").replace("{", "{\n ").replace("}", "\n}")

def get_state_string():
    global leader_pid, ballot_num, accept_num, accept_val, broken_streams, acks
    result = f"Leader PID: {leader_pid}\n"
    result += f"Ballot Num: {ballot_num}\n"
    result += f"Accept Num: {accept_num}\n"
    result += f"Accept Val: {accept_val}\n"
    result += f"Broken Channels: \n{get_object_string(broken_streams)}\n"
    result += f"Acknowledgements: {acks}\n"
    return result

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

def log(message):
    print(f"[INFO] {message}\n", flush=True)

def direct_message(message, stream, delay = MESSAGE_DELAY):
    helpers.broadcast_message(f"server {pid} -> {message}", [stream], delay)

def forward_message(message, stream, delay = MESSAGE_DELAY):
    helpers.broadcast_message(f"client -> {message}", [stream], delay)

# could include processor id in message if we needed
# general broadcasting function prepends [pid] before all messages
def broadcast_message(message, delay = MESSAGE_DELAY):
    global sock_out1, sock_out2, inputStreams
    helpers.broadcast_message(f"server {pid} -> {message}", [sock_out1, sock_out2] + inputStreams, delay)

# ex. prepare - 3,2
def broadcast_prepare():
    global ballot_num, blockchain
    broadcast_message(f"prepare{PAYLOAD_DELIMITER}{ballot_num[0]},{ballot_num[1]}{PAYLOAD_DELIMITER}{len(blockchain)}")

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
        log(f"({dest})--- X ---({src})")
        update_broken_streams(src, True)

def handle_received_fixLink(src,dest):
    global pid
    if pid == dest:
        log(f"({dest})--->->->---({src})")
        update_broken_streams(src, False)

# Begin Paxos functions
def handle_received_prepare(received_ballot_num, received_depth, stream, id):
    global ballot_num, blockchain
    if received_ballot_num >= ballot_num and received_depth >= len(blockchain):
        set_ballot_num(received_ballot_num)
        set_leader_pid(id)
        set_leader_stream(stream)
        send_promise(stream)

def handle_received_promise(received_accept_num, received_accept_val):
    global highest_received_ballot
    if received_accept_num > highest_received_ballot and received_accept_val != "None" and len(received_accept_val) > 0:
        set_highest_received_ballot(received_accept_num)
        set_highest_received_val(received_accept_val)
    
    increment_acks()

def handle_received_accept(received_ballot_num, received_accept_val, sender_pid, stream):
    global blockchain, ballot_num
    if received_ballot_num >= ballot_num:
        set_leader_pid(sender_pid)
        set_leader_stream(stream)
        set_ballot_num((received_ballot_num[0], pid))
        set_accept_num(received_ballot_num)
        block = bc.parse_block_from_payload(received_accept_val)
        
        if len(blockchain) > 0 and block.prev_hash != bc.hash_block(blockchain[-1]):
            # this server is out of sync and behind, ask leader for up-to-date blockchain
            reset_accept_val()
            reset_accept_num()
            direct_message(f"blockchain{PAYLOAD_DELIMITER}sync-request", stream)
        else:
            set_accept_val(block)

        send_accepted(stream)

def handle_received_accepted():
    increment_acks()

def handle_received_decide(received_ballot_num, received_accept_val):
    global accept_num
    
    set_ballot_num((received_ballot_num[0], pid))
    set_accept_num(received_ballot_num)

    block = bc.parse_block_from_payload(received_accept_val)
    
    # might be deprecated
    if len(blockchain) > 0 and bc.hash_block(blockchain[-1]) == bc.hash_block(block):
        reset_accept_num()
        reset_accept_val()
        return

    append_to_blockchain(block)
    reset_accept_num()
    reset_accept_val()
# End Paxos functions

def handle_blockchain_reconstruct(id = pid, callback = None):
    global Blockchain
    
    reset_blockchain()
    persisted_blockchain = bc.reconstruct(id)
    for b in persisted_blockchain:
        append_to_blockchain(b)
    
    if callback:
        callback()

def handle_operations_queue():
    global temporary_operations, accept_val, new_election

    while True:
        # wait until next paxos run
        wait = 0
        while not new_election and (accept_val != None or len(temporary_operations.queue) == 0):
            wait += 1

        next_op = temporary_operations.get()
        threading.Thread(target=begin_paxos,
                                    args=(next_op[0],next_op[1]), daemon=True).start()

def server_communications(stream):
    global pid, blockchain, database, broken_streams, sock_in1, sock_out1, sock_out2, leader_pid, leader_stream, new_election, temporary_operations
    addr = stream.getsockname()
    while True:
        data = stream.recv(1024)
        if data:
            decoded = data.decode(encoding)
            # example received message = server -> payload'
            tokens = decoded.split(" -> ")
            sender_tokens = tokens[0].split(" ")
            sender = sender_tokens[0]
            payload = tokens[1]
            if sender == "client":
                payload_tokens = payload.split(PAYLOAD_DELIMITER)
                command = payload_tokens[0]

                if command == "leader":
                    log(f"received leader tag")
                    if leader_pid != pid:
                        log(f"forcing new election")
                        set_new_election(True)
                    payload_tokens = payload_tokens[1:]
                    command = payload_tokens[0]
                    payload = PAYLOAD_DELIMITER.join(payload_tokens)
                
                if leader_stream != None and not new_election:
                    log(f'Forward Payload: {payload}')
                    direct_message(f"leader{PAYLOAD_DELIMITER}{leader_pid}", stream)
                    threading.Thread(target=forward_message,
                                    args=(payload,leader_stream), daemon=True).start()
                    continue
                
                # become leader
                if(leader_pid != pid):
                    set_leader_pid(pid)
                    set_leader_stream(None)
                    set_new_election(True)
            
                print(f'[C]: {payload}\n', flush=True)
                # example payload = put - student_id1 - '858-123-4567'
                if command == "broadcast":
                    sentence = payload_tokens[1].strip("'")
                    threading.Thread(target=broadcast_message,
                                    args=(sentence,0), daemon=True).start()
                    payload_tokens = sentence.split(PAYLOAD_DELIMITER)
                    command = payload_tokens[0]
                if command == "down" or command == "failProcess":
                    time.sleep(0.1)
                    helpers.handle_exit([sock_in1, sock_out1, sock_out2])
                    return
                if command == "put":
                    key = payload_tokens[1]
                    value = payload_tokens[2].strip("'")

                    def callback(block):
                        broadcast_message(f"resp{PAYLOAD_DELIMITER}{block.operation.command}{PAYLOAD_DELIMITER}{block.operation.key}{PAYLOAD_DELIMITER}{block.operation.value}")

                    #append_operation((bc.Operation(cmd=command, key=key, value=value), callback))
                    temporary_operations.put((bc.Operation(cmd=command, key=key, value=value), callback))
                elif command == "get":
                    key = payload_tokens[1]

                    def callback(block):
                        global database
                        if block.operation.key in database:
                            broadcast_message(f"resp{PAYLOAD_DELIMITER}{block.operation.command}{PAYLOAD_DELIMITER}{block.operation.key}{PAYLOAD_DELIMITER}{database[block.operation.key]}")
                        else:
                            broadcast_message(f"resp{PAYLOAD_DELIMITER}{block.operation.command}{PAYLOAD_DELIMITER}{block.operation.key}{PAYLOAD_DELIMITER}NO_KEY")

                    #append_operation((bc.Operation(cmd=command, key=key, value=None), callback))
                    temporary_operations.put((bc.Operation(cmd=command, key=key, value=None), callback))
                elif command == "p" or command == "persist":
                    threading.Thread(target=bc.persist,
                                    args=(pid, blockchain), daemon=True).start()
                elif command == "r" or command == "reconstruct":
                    threading.Thread(target=handle_blockchain_reconstruct,
                                    args=(), daemon=True).start()
                elif command == "exit":
                    helpers.handle_exit([inputStreams[0], inputStreams[1], sock_out1, sock_out2])
                elif command == "bc" or command == "blockchain":
                    log(f"--- [BC Head] ---\n{bc.print_blockchain(blockchain)}--- [BC Tail] ---")
                elif command == "db" or command == "database":
                    log(f"Database: {get_object_string(database)}")
                elif command == "state":
                    log(f"--- State ---\n{get_state_string()}-------------")
            elif sender == "server":
                sender_pid = int(sender_tokens[1])
                # example payload = promise - 3,2, - 2,2 - put 'hello world'
                payload_tokens = payload.split(PAYLOAD_DELIMITER)
                command = payload_tokens[0]
                
                if command == "resp" or command == "leader":
                    continue

                if command == "down" or command == "failProcess":
                    helpers.handle_exit([sock_in1, sock_out1, sock_out2])
                    return

                if command == "fix" or command == "fixLink":
                    src = int(payload_tokens[1])
                    dest = int(payload_tokens[2])
                    threading.Thread(target=handle_received_fixLink,
                            args=(src, dest), daemon=True).start()
                    continue
                else:
                    if broken_streams[sender_pid] == True:
                        continue
                
                if command == "fail" or command == "failLink":
                    src = int(payload_tokens[1])
                    dest = int(payload_tokens[2])
                    threading.Thread(target=handle_received_failLink,
                            args=(src, dest), daemon=True).start()
                    continue
                
                print(f'[P{sender_pid}]: {payload}\n', flush=True)
                
                if command == "prepare":
                    received_ballot_num = tuple(int(e) for e in payload_tokens[1].split(","))
                    received_depth = int(payload_tokens[2])

                    threading.Thread(target=handle_received_prepare,
                                    args=(received_ballot_num,received_depth,stream,sender_pid), daemon=True).start()
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
                                    args=(received_ballot_num,received_accept_val,sender_pid,stream), daemon=True).start()
                elif command == "accepted":
                    threading.Thread(target=handle_received_accepted,
                                    args=(), daemon=True).start()
                elif command == "decide":
                    received_ballot_num = tuple(int(e) for e in payload_tokens[1].split(","))
                    received_accept_val = PAYLOAD_DELIMITER.join(payload_tokens[2:])

                    threading.Thread(target=handle_received_decide,
                                    args=(received_ballot_num, received_accept_val), daemon=True).start()
                elif command == "p" or command == "persist":
                    threading.Thread(target=bc.persist,
                                    args=(pid, blockchain), daemon=True).start()
                elif command == "r" or command == "reconstruct":
                    threading.Thread(target=handle_blockchain_reconstruct,
                                    args=(), daemon=True).start()
                elif command == "bc" or command == "blockchain":
                    if len(payload_tokens) > 1:
                        action = payload_tokens[1]
                        if action == "sync-request":
                            # persist blockchain data and send ready to other server to sync
                            def callback():
                                direct_message(f"blockchain{PAYLOAD_DELIMITER}sync-ready", stream)
                            threading.Thread(target=bc.persist,
                                            args=(pid, blockchain, callback), daemon=True).start()
                        elif action == "sync-ready":
                            set_leader_pid(sender_pid)
                            set_leader_stream(stream)
                            def callback():
                                if accept_val:
                                    append_to_blockchain(accept_val)
                            threading.Thread(target=handle_blockchain_reconstruct,
                                            args=(sender_pid,callback), daemon=True).start()
                    else:
                        log(f"--- [BC Head] ---\n{bc.print_blockchain(blockchain)}--- [BC Tail] ---")
                elif command == "db" or command == "database":
                    log(f"Database: {get_object_string(database)}")
                elif command == "state":
                    log(f"--- State ---\n{get_state_string()}-------------")
        else:
            break

#######################################################################
# LEADER ELECTION 
# 1. Server is alive
# 2. If server receives message from client, begin leader election
#######################################################################
def begin_paxos(proposed_operation, callback):
    global pid, new_election, blockchain, acks, ballot_num, accept_val, highest_received_val, leader_pid

    reset_acks()
    reset_highest_received_ballot()
    reset_highest_received_val()
    increment_ballot_num()

    proposed_ballot_num = ballot_num

    log(f"Phase 1 Leader PID = {leader_pid}")

    # phase 1
    # if leader_stream is None, then leader election must take place
    if new_election and leader_pid == pid:
        set_new_election(False)
        broadcast_prepare()
        timeout_time = time.time() + 5  # timeout after 5 seconds
        while (acks < max_clients / 2):
            if time.time() >= timeout_time:
                log(f"ballot {ballot_num} timed out during phase 1")
                return
        
    prev_block = None
    if len(blockchain) > 0:
        prev_block = blockchain[-1]
    block = bc.Block(op=proposed_operation, prev_block=prev_block)
    block.mine()

    # ballot recovery
    if highest_received_val != None:
        log(f"recovered {highest_received_val} from previous paxos sequence")
        block = bc.parse_block_from_payload(highest_received_val)

    log(f"Phase 2 Leader PID = {leader_pid}")

    # phase 2
    if proposed_ballot_num != ballot_num and leader_pid != None and leader_stream != None:
        log(f'{proposed_ballot_num} < {ballot_num}, Forward Payload: {proposed_operation.to_payload()}')
        broadcast_message(f"leader{PAYLOAD_DELIMITER}{leader_pid}")
        threading.Thread(target=forward_message,
                        args=(proposed_operation.to_payload(),leader_stream), daemon=True).start()           
        reset_accept_val()
        return

    set_accept_val(block)

    reset_acks()
    broadcast_accept()
    timeout_time = time.time() + 5  # timeout after 5 seconds
    while (acks < max_clients / 2):
        if time.time() >= timeout_time:
            log(f"ballot {ballot_num} timed out during phase 2")
            reset_accept_val()
            return
    
    append_to_blockchain(accept_val)
    callback(block)
    
    #pop_operation()

    # phase 3
    broadcast_decide()

    # reset
    reset_acks()
    reset_accept_num()
    reset_accept_val()
    reset_highest_received_ballot()
    reset_highest_received_val()

# accept any incoming socket connection and create a thread to handle communication on this new stream
def accept_connections():
    while True:
        stream, addr = sock_in1.accept()
        inputStreams.append(stream)
        log(f'Connected to {addr}')
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
    broadcast_message(f"failLink{PAYLOAD_DELIMITER}{src}{PAYLOAD_DELIMITER}{dest}", 0)
    handle_received_failLink(dest, src)
    update_broken_streams(dest, True)

def handle_fixLink(src, dest):
    broadcast_message(f"fixLink{PAYLOAD_DELIMITER}{src}{PAYLOAD_DELIMITER}{dest}", 0)
    handle_received_fixLink(dest, src)
    update_broken_streams(dest, False)

def input_listener():
    global pid, blockchain
    user_input = input()
    while True:
        tokens = user_input.split(" ")
        command = tokens[0]
        if command == "down" or command == "failProcess":
            helpers.handle_exit([sock_in1, sock_out1, sock_out2])
            return
        elif command == "p" or command == "persist":
                    threading.Thread(target=bc.persist,
                                    args=(pid, blockchain), daemon=True).start()
        elif command == "r" or command == "reconstruct":
            threading.Thread(target=handle_blockchain_reconstruct,
                            args=(), daemon=True).start()
        elif command == "fail" or command == "failLink":
            src = int(tokens[1])
            dest = int(tokens[2])
            threading.Thread(target=handle_failLink,
                     args=(src, dest), daemon=True).start()
        elif command == "fix" or command == "fixLink":
            src = int(tokens[1])
            dest = int(tokens[2])
            threading.Thread(target=handle_fixLink,
                     args=(src, dest), daemon=True).start()
        elif command == "bc" or command == "blockchain":
            if len(tokens) > 1:
                block_index = int(tokens[1])
                log(f"Block [{block_index}]: {blockchain[block_index]}")
            else:
                log(f"Blockchain: {bc.print_blockchain(blockchain)}")
        elif command == "state":
            log(f"--- State ---\n{get_state_string()}-------------")
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
import helpers

import socket
import os
import csv
import threading
import sys
import queue
import time
import csv

IP = socket.gethostname()
encoding = 'utf-8'
port_base = 6000
pid = int(sys.argv[1])  # pid = 1, 2, 3, 4, or 5
max_clients = 4

leader = pid
leader_lock = threading.Lock()

def set_leader(new_leader):
    global leader
    leader_lock.acquire()
    leader = new_leader
    leader_lock.release()

leader_acks = 0
leader_acks_lock = threading.Lock()

def increment_acks():
    global leader_acks
    leader_acks_lock.acquire()
    leader_acks += 1
    leader_acks_lock.release()

def set_acks(new_acks):
    global leader_acks
    leader_acks_lock.acquire()
    leader_acks = new_acks
    leader_acks_lock.release()

# sending socket 1
sock_out1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_out1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

# sending socket 2
sock_out2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_out2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

# listening socket
sock_in1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_in1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock_in1.bind((IP, port_base + pid))
sock_in1.listen(2)
inputStreams = []

broadcast_lock = threading.Lock()

def broadcast_message(message):
    global inputStreams, sock_out1, sock_out2
    broadcast_lock.acquire()

    time.sleep(0.5)
    for sock in inputStreams:
        try:
            sock.sendall(str.encode(message))
        except:
            print(f'failed to send {message} to {sock.getsockname()}', flush=True)

    try:
        sock_out1.sendall(str.encode(message))
    except:
        print(f'failed to send {message} to {sock_out1.getsockname()}', flush=True)

    try:
        sock_out2.sendall(str.encode(message))
    except:
        print(f'failed to send {message} to {sock_out2.getsockname()}', flush=True)

    broadcast_lock.release()

# TA how to prioritize
temporary_operations = queue.PriorityQueue(maxsize=0)
database = {}
blockchain = []

# str(Operation) => <put,someKey,someValue>
# str(Block) => <put,someKey,someValue> someReallyLongHash1283812312 35
def persist():
    with open('blockchain.csv', 'w', newline='') as csvfile:
        fieldnames = ['operations', 'prev_hash', 'nonce']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for block in blockchain:
            writer.writerow(block.to_csv())
        
        block1 = helpers.Block(helpers.Operation("put", "key1", "value1"), None)
        block1.mine()
        block2 = helpers.Block(helpers.Operation("get", "key1", str(None)), block1)
        block2.mine()
        writer.writerow(block1.to_csv())
        writer.writerow(block2.to_csv())

def reconstruct():
    global blockchain
    with open('blockchain.csv', newline='') as csvfile:
        blocks = csv.reader(csvfile, delimiter=',', quotechar='|')
        firstBlock = True
        for block in blocks:
            if firstBlock:
                firstBlock = False
                continue
            operationTokens = (block[0])[1:-1].split(" ")
            operation = helpers.Operation(operationTokens[0], operationTokens[1], operationTokens[2])
            blockchain.append(helpers.Block(operation, block[1], block[2]))
            print(f'Added block: {blockchain[-1]}', flush=True)
    print(f'Reconstructed blockchain: {blockchain}', flush=True)

def set_current_leader(leader_pid):
    global leader
    leader_lock.acquire()
    leader = leader_pid
    leader_lock.release()



leader_stream = None
leader_stream_lock = threading.Lock()
def set_leader_stream(stream):
    global leader_stream
    leader_stream_lock.acquire()
    leader_stream = stream
    leader_stream_lock.release()

# handles RL and CL
def determine_leader(leader_pid, stream):
    global leader
    leader_lock.acquire()

    time.sleep(0.5)
    if leader_pid > leader:
        leader = leader_pid
        set_leader_stream(stream)
        stream.sendall(str.encode(f"server AL"))
        print(f"New leader selected: PID = {leader_pid}")
    elif leader_pid < leader:
        stream.sendall(str.encode(f"server CL {leader}"))
    
    leader_lock.release()

def send_leader_to_client_stream():
    global inputStreams
    if len(inputStreams) <= 2:
        return
    
    broadcast_message(f"server NL {leader}")

def send_client_leader_pid(stream):
    global leader
    time.sleep(0.5)
    stream.sendall(str.encode(f"server CL {leader}"))

def server_communications(stream):
    global leader, pid, leader_stream
    addr = stream.getsockname()
    while True:
        data = stream.recv(1024)
        if not data and leader_stream == stream:
            set_leader(pid)
            threading.Thread(target=request_leadership, args=(), daemon=True).start()
            break
        if data:
            decoded = data.decode(encoding)
            print(f'Data received ({decoded}) from {addr}', flush=True)
            tokens = decoded.split(" ", 2)
            # print(f'Data -> Tokens: {tokens}', flush=True)
            sender = tokens[0]
            command = tokens[1]
            # example command -> client broadcast 'hello world'
            if sender == "client":
                if leader != pid:
                    leader_stream.sendall(data)
                    stream.sendall(str.encode(f"server NL {leader}"))
                    break
                if command == "broadcast":
                    sentence = tokens[2].strip("'")
                    threading.Thread(target=broadcast_message,
                                    args=(sentence,), daemon=True).start()
                elif command == "exit":
                    helpers.handle_exit([inputStreams[0], inputStreams[1], sock_out1, sock_out2])
                elif command == "persist":
                    persist()
                elif command == "put":
                    key = tokens[2]
                    value = tokens[3].strip("'")
                    database[key] = value
                    temporary_operations.put(helpers.Operation(command, key, value))
                    print(f'Updated database: {database}', flush=True)
                elif command == "get":
                    key = tokens[2]
                    print(f'Database value for {key} = {database.get(key)}', flush=True)
                    temporary_operations.put(helpers.Operation(command, key, None))
                elif command == "reconstruct":
                    reconstruct()
                elif command == "leader":
                    threading.Thread(target=send_client_leader_pid,
                                    args=(stream,), daemon=True).start()
            elif sender == "server":
                # request leadership
                if command == "RL":
                    leader_pid = int(tokens[2])
                    threading.Thread(target=determine_leader,
                                    args=(leader_pid,stream), daemon=True).start()
                # acknowledge leader
                elif command == "AL":
                    threading.Thread(target=increment_acks, args=(), daemon=True).start()
                # new leader was chosen
                elif command == "NL":
                    leader_pid = int(tokens[2])
                    threading.Thread(target=determine_leader,
                                    args=(leader_pid,stream), daemon=True).start()
                # current leader
                elif command == "CL":
                    leader_pid = int(tokens[2])
                    if(leader < leader_pid):
                        threading.Thread(target=determine_leader,
                                        args=(leader_pid,stream), daemon=True).start()
        else:
            break

def accept_connections():
    global leader
    # helpers.print_expecting_connections(pid, port_base)
    while True:
        stream, addr = sock_in1.accept()
        inputStreams.append(stream)
        print(f'Connected to {addr}', flush=True)
        stream.sendall(str.encode(f"server NL {leader}"))
        threading.Thread(target=server_communications,
                         args=(stream,), daemon=True).start()

#sock_out1, sock_out2, inputStreams
def request_leadership():
    global leader, pid, leader_acks

    ack_wait = 0
    while (leader_acks < max_clients / 2):
        # print(f"leader_acks = {leader_acks}, max_clients/2 = {max_clients/2}")
        if leader > pid:
            return
        ack_wait += 1

        broadcast_message(f"server RL {pid}")
        time.sleep(1)
        # maybe sleep after awhile

    broadcast_message(f"server NL {pid}")

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


threading.Thread(target=accept_connections, args=()).start()
threading.Thread(target=send_connections, args=()).start()
threading.Thread(target=request_leadership, args=()).start()

while True:
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        helpers.handle_exit([sock_in1, sock_out1, sock_out2])
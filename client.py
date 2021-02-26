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
connected_clients = 0
max_clients = 4

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

temporary_operations = queue.PriorityQueue(maxsize=0)
database = {}
blockchain = []

def broadcast_message(message):
    global inputStreams, sock_out1, sock_out2
    for sock in inputStreams:
        sock.sendall(str.encode(message))
    sock_out1.sendall(str.encode(message))
    sock_out2.sendall(str.encode(message))

# str(Operation) => <put,someKey,someValue>
# str(Block) => <put,someKey,someValue> someReallyLongHash1283812312 35
def persist():
    with open('blockchain.csv', 'w', newline='') as csvfile:
        fieldnames = ['operations', 'prev_hash', 'nonce']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for block in blockchain:
            writer.writerow(block.to_csv())


def input_listener():
    global inputStreams, blockchain
    user_input = input()
    while True:
        tokens = user_input.split(" ", 2)
        # print(f'[PID = {pid}] Received input: {user_input}, tokens = {tokens}')
        command = tokens[0]
        # broadcast 'hello world'
        if command == "broadcast":
            sentence = tokens[1].strip("'")
            threading.Thread(target=broadcast_message,
                             args=(sentence,), daemon=True).start()
        elif command == "exit":
            helpers.handle_exit([inputStreams[0], inputStreams[1], sock_out1, sock_out2])
        elif command == "persist":
            op1 = helpers.Operation("get", "steve", "vikram")
            block1 = helpers.Block(op1, None)
            block1.mine()
            blockchain.append(block1)
            persist()
        elif command == "put":
            key = tokens[1]
            value = tokens[2].strip("'")
            database[key] = value
            temporary_operations.put(helpers.Operation(command, key, value))
            print(f'Updated database: {database}', flush=True)
        elif command == "get":
            key = tokens[1]
            print(f'Database value for {key} = {database.get(key)}', flush=True)
            temporary_operations.put(helpers.Operation(command, key, None))

        user_input = input()

def client_communications(stream):
    addr = stream.getsockname()
    while True:
        data = stream.recv(1024)
        if data:
            decoded = data.decode(encoding)
            print(f'Data received ({decoded}) from {addr}', flush=True)
        else:
            break

def accept_connections():
    global connected_clients
    helpers.print_expecting_connections(pid, port_base)
    while (connected_clients < max_clients):
        stream, addr = sock_in1.accept()
        inputStreams.append(stream)
        print(f'Connected to {addr}', flush=True)
        connected_clients+=1
        threading.Thread(target=client_communications,
                         args=(stream,), daemon=True).start()

threading.Thread(target=accept_connections, args=()).start()
# threading.Thread(target=accept_connections, args=(sock_in2), daemon=True).start()

def send_connections():
    global connected_clients
    out_addr1, out_addr2 = helpers.get_output_connection_tuples(pid, IP, port_base)
    while (connected_clients < max_clients):
        sock_out1_result = sock_out1.connect_ex(out_addr1)
        sock_out2_result = sock_out2.connect_ex(out_addr2)
        if sock_out1_result == 0:
            threading.Thread(target=client_communications,
                     args=(sock_out1,), daemon=True).start()
            connected_clients += 1
        if sock_out2_result == 0:
            threading.Thread(target=client_communications,
                     args=(sock_out2,), daemon=True).start()
            connected_clients += 1
        time.sleep(0.5)

threading.Thread(target=send_connections, args=()).start()

threading.Thread(target= input_listener, args=()).start()
while True:
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        helpers.handle_exit([sock_in1, sock_out1, sock_out2])
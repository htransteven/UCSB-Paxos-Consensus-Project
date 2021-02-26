import helpers

import socket
import os
import threading
import sys
import queue
import time

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

def broadcast_message(message):
    global inputStreams, sock_out1, sock_out2
    for sock in inputStreams:
        sock.sendall(str.encode(message))
    sock_out1.sendall(str.encode(message))
    sock_out2.sendall(str.encode(message))

def input_listener():
    global inputStreams
    user_input = input()
    while True:
        tokens = user_input.split(" ", 1)
        # print(f'[PID = {pid}] Received input: {user_input}, tokens = {tokens}')
        command = tokens[0]
        # broadcast 'hello world'
        if command == "broadcast":
            sentence = tokens[1].strip("'")
            threading.Thread(target=broadcast_message,
                             args=(sentence,), daemon=True).start()
        elif command == "exit":
            helpers.handle_exit([inputStreams[0], inputStreams[1], sock_out1, sock_out2])
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
import helpers

import socket
import os
import threading
import sys
import queue
import time

IP = socket.gethostname()
port_base = 6000
pid = int(sys.argv[1])  # pid = 1, 2, 3, 4, or 5
# port_server = int(sys.argv[2])  # server port
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


def accept_connections():
    global connected_clients
    if pid == 1:
        print(f'Expecting connection from {port_base + 4}', flush=True)
        print(f'Expecting connection from {port_base + 2}', flush=True)
    elif pid == 2:
        print(f'Expecting connection from {port_base + 4}', flush=True)
        print(f'Expecting connection from {port_base + 5}', flush=True)
    elif pid == 3:
        print(f'Expecting connection from {port_base + 2}', flush=True)
        print(f'Expecting connection from {port_base + 1}', flush=True)
    elif pid == 4:
        print(f'Expecting connection from {port_base + 3}', flush=True)
        print(f'Expecting connection from {port_base + 5}', flush=True)
    elif pid == 5:
        print(f'Expecting connection from {port_base + 1}', flush=True)
        print(f'Expecting connection from {port_base + 3}', flush=True)
    while (connected_clients < max_clients):
        stream, addr = sock_in1.accept()
        inputStreams.append(stream)
        print(f'Connected to {addr}', flush=True)
        connected_clients+=1
        # Spawn the thread to handle recvdata
        #threading.Thread(target=client_communications,
        #                 args=(stream, addr), daemon=True).start()

threading.Thread(target=accept_connections, args=()).start()
# threading.Thread(target=accept_connections, args=(sock_in2), daemon=True).start()

# connection flow = every processor sends out 2 requests and listens for 2
# p1 connects to p3 and p5, p1 listens for p4 and p2
# p3 connects to p5 and p4, p3 listens for p2 and p1
# p5 connects to p4 and p2, p5 listens for p1 and p3
# p4 connects to p2 and p1, p4 listens for p3 and p5
# p2 connects to p1 and p3, p2 listens for p4 and p5

def send_connections():
    global connected_clients
    if pid == 1:
        sock_out1.connect_ex((IP, port_base + 3))
        sock_out2.connect_ex((IP, port_base + 5))
    elif pid == 2:
        sock_out1.connect_ex((IP, port_base + 1))
        sock_out2.connect_ex((IP, port_base + 3))
    elif pid == 3:
        sock_out1.connect_ex((IP, port_base + 5))
        sock_out2.connect_ex((IP, port_base + 4))
    elif pid == 4:
        sock_out1.connect_ex((IP, port_base + 2))
        sock_out2.connect_ex((IP, port_base + 1))
    elif pid == 5:
        sock_out1.connect_ex((IP, port_base + 4))
        sock_out2.connect_ex((IP, port_base + 2))
    connected_clients += 2
#     # Spawn the thread to handle recvdata
#     #threading.Thread(target=client_communications,
#     #                 args=(sock_clientA, sock_clientA.getsockname()), daemon=True).start()

threading.Thread(target=send_connections, args=()).start()

while True:
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        helpers.handle_exit([sock_in1, sock_out1, sock_out2])
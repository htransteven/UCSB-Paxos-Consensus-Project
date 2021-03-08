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
leader_pid = 5

leader_pid_lock = threading.Lock()

def set_leader_pid(new_leader_pid):
    global leader_pid
    leader_pid_lock.acquire()
    leader_pid = new_leader_pid
    leader_pid_lock.release()

def decrement_leader_pid():
    global leader_pid
    leader_pid_lock.acquire()
    if leader_pid == 1:
        leader_pid = 5
    else:
        leader_pid -= 1
    leader_pid_lock.release()

# connect to leader
sock_out = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_out.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
def server_communications(stream):
    addr = stream.getsockname()
    stream.sendall(str.encode(f"client leader"))
    while True:
        data = stream.recv(1024)
        if data:
            decoded = data.decode(encoding)
            print(f'Data received ({decoded}) from {addr}', flush=True)
            tokens = decoded.split(" ", 2)

            sender = tokens[0]
            command = tokens[1]

            if sender == "server":
                if command == "NL":
                    leader_pid = tokens[2]
                    threading.Thread(target=set_leader_pid,
                                    args=(leader_pid,), daemon=True).start()

def connect_to_leader():
    sock_out_result = sock_out.connect_ex((IP,port_base + leader_pid))
    while (sock_out_result != 0):
        decrement_leader_pid()
        sock_out_result = sock_out.connect_ex((IP,port_base + leader_pid))
    
    threading.Thread(target=server_communications,
                args=(sock_out,), daemon=True).start()

threading.Thread(target=connect_to_leader, args=()).start()

def input_listener():
    global inputStreams, blockchain
    user_input = input()
    while True:
        sock_out.sendall(str.encode("client " + user_input))
        user_input = input()

threading.Thread(target=input_listener, args=()).start()

while True:
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        helpers.handle_exit([sock_out])
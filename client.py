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

max_servers = 5



servers_list = []
for i in range(max_servers + 1):
    if i == 0:
        servers_list.append(None)
        continue
    sock_server_i = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock_server_i.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock_server_i.connect_ex((IP,port_base + i))
    servers_list.append(sock_server_i)

leader_pid = max_servers
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
    
def server_communications(stream, stream_pid):
    global leader_pid

    addr = stream.getsockname()
    stream.connect_ex((IP,port_base + stream_pid))

    while True:
        if stream_pid != leader_pid:
            return
        try:
            data = stream.recv(1024)
        except socket.error as e:
            decrement_leader_pid()
            connect_to_leader()
            break
        if not data:
            decrement_leader_pid()
            connect_to_leader()
            break

        if data:
            decoded = data.decode(encoding)
            print(f'Data received ({decoded}) from {addr}', flush=True)
            tokens = decoded.split(" ", 2)

            sender = tokens[0]
            command = tokens[1]

            if sender == "server":
                if command == "CL" or command == "NL":
                    pid_param = int(tokens[2])
                    threading.Thread(target=switch_leader,
                                    args=(pid_param,), daemon=True).start()
                    break

def switch_leader(new_leader_pid):
    set_leader_pid(new_leader_pid)
    threading.Thread(target=server_communications,
                args=(servers_list[new_leader_pid],new_leader_pid), daemon=True).start()

def connect_to_leader():
    global leader_pid
    print(f"leader_pid = {leader_pid}, servers = {len(servers_list)}")
    threading.Thread(target=server_communications,
                args=(servers_list[leader_pid], leader_pid), daemon=True).start()

threading.Thread(target=connect_to_leader, args=()).start()

def input_listener():
    global inputStreams, blockchain, servers_list, leader_pid, max_servers
    user_input = input()
    while True:
        if user_input == "whois leader":
            print(f"I think the leader is server {leader_pid}")
        else:
            try:
                servers_list[leader_pid].sendall(str.encode("client " + user_input))
            except Exception as e:
                decrement_leader_pid()
                connect_to_leader()
        user_input = input()

threading.Thread(target=input_listener, args=()).start()

while True:
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        helpers.handle_exit(servers_list)
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
leader_pid = 1

# connect to leader
sock_out = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_out.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock_out_result = sock_out.connect_ex((IP,port_base + leader_pid))

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
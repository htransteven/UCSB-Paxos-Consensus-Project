import helpers
from helpers import PAYLOAD_DELIMITER

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
min_pid = 1
max_pid = 5
TIMEOUT_LENGTH = 10

start_time = time.time()

def log(message):
    print(f"[{round(time.time() - start_time, 2)} - INFO] {message}")

def direct_message(message, stream, delay = 0.2):
    helpers.broadcast_message(f"client -> {message}", [stream], delay)

leader = None
leader_lock = threading.Lock()
leader_stream = None
leader_stream_lock = threading.Lock()

def set_leader(new_leader):
    global leader, leader_lock
    leader_lock.acquire()
    leader = new_leader
    leader_lock.release()

def decrement_leader():
    global leader, min_pid, max_pid
    if leader == min_pid:
        set_leader(max_pid)
    else:
        set_leader(leader - 1)

def set_leader_stream(stream):
    global leader_stream, leader_stream_lock
    leader_stream_lock.acquire()
    leader_stream = stream
    leader_stream_lock.release()

last_sent_command = None
last_sent_last_sent_command_lock = threading.Lock()

def set_last_sent_command(cmd):
    global last_sent_command, last_sent_last_sent_command_lock
    last_sent_last_sent_command_lock.acquire()
    last_sent_command = cmd
    last_sent_last_sent_command_lock.release()
    
def server_communications(stream, stream_pid):
    global leader, start_time

    addr = stream.getsockname()
    while True:
        if stream_pid != leader:
            return

        try:
            data = stream.recv(1024)
        except socket.error as e:
            connect_to_leader()
            break
        if not data:
            connect_to_leader()
            break

        if data:
            decoded = data.decode(encoding)
            
            tokens = decoded.split(" -> ")
            sender_tokens = tokens[0].split(" ")
            sender = sender_tokens[0]
            payload = tokens[1]
            if sender == "server":
                sender_pid = int(sender_tokens[1])
                payload_tokens = payload.split(PAYLOAD_DELIMITER)
                command = payload_tokens[0]
                if command == "get" or command == "put":
                    set_last_sent_command(None)
                    key = payload_tokens[1]
                    value = payload_tokens[2]
                    print(f'[{round(time.time() - start_time, 2)} - P{sender_pid}]: {key} = {value}', flush=True)
                else:
                    continue

def resend_payload(payload):
    global leader, last_sent_command, leader_stream

    timeout = time.time() + TIMEOUT_LENGTH
    while timeout >= time.time():
        if last_sent_command == None:
            return
    
    log(f"no response from {leader} after {TIMEOUT_LENGTH} seconds")
    decrement_leader()
    connect_to_leader()
    log(f"resending {payload} to {leader}")
    direct_message(payload, leader_stream)
    threading.Thread(target=resend_payload, args=(payload,), daemon=True).start()
        

def input_listener():
    global leader_stream
    user_input = input()
    while True:
        if user_input == "leader":
            log(f"I think the leader is server {leader}")
        else:
            message = PAYLOAD_DELIMITER.join(user_input.split(" ", 2))
            try:
                message_tokens = message.split(PAYLOAD_DELIMITER)
                cmd = message_tokens[0]
                if cmd == "put" or cmd == "get":
                    set_last_sent_command(cmd)

                direct_message(message, leader_stream)
                threading.Thread(target=resend_payload, args=(message,), daemon=True).start()
            except Exception as e:
                connection_result = connect_to_leader()
                if connection_result == None:
                    break

        user_input = input()

def connect_to_server(pid):
    sock_server_pid = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock_server_pid.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    connection_result = sock_server_pid.connect_ex((IP,port_base + pid))
    if connection_result != 0:
        return None
    
    return sock_server_pid

def connect_to_leader():
    global leader, servers, max_pid

    if leader == None:
        set_leader(max_pid)

    cycle_start = leader
    first_attempt = True

    while leader != cycle_start or first_attempt:
        if first_attempt:
            first_attempt = False

        leader_stream = connect_to_server(leader)
        if leader_stream != None:
            set_leader_stream(leader_stream)
            threading.Thread(target=server_communications,
            args=(leader_stream, leader), daemon=True).start()
            threading.Thread(target=input_listener, args=(), daemon=True).start()
            return leader_stream
        decrement_leader()
    else:
        log(f"all servers are down, shutting down client...")
        helpers.handle_exit([leader_stream])
        return None
    

        
threading.Thread(target=connect_to_leader, args=()).start()

while True:
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        helpers.handle_exit([leader_stream])
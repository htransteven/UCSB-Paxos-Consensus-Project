import helpers
from helpers import PAYLOAD_DELIMITER
import blockchain as bc

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
TIMEOUT_LENGTH = 30

start_time = time.time()

def log(message):
    print(f"[{round(time.time() - start_time, 2)} - INFO] {message}\n")

def direct_message(message, stream, delay = 1):
    helpers.broadcast_message(f"client -> {message}", [stream], delay)

leader_pid = None
leader_pid_lock = threading.Lock()
leader_stream = None
leader_stream_lock = threading.Lock()

def set_leader_pid(new_leader):
    global leader_pid, leader_pid_lock
    leader_pid_lock.acquire()
    leader_pid = new_leader
    leader_pid_lock.release()

def decrement_leader_pid():
    global leader_pid, min_pid, max_pid
    if leader_pid == min_pid:
        set_leader_pid(max_pid)
    else:
        set_leader_pid(leader_pid - 1)

def set_leader_stream(stream):
    global leader_pid, leader_stream, leader_stream_lock
    leader_stream_lock.acquire()
    log(f"set leader_pid to {leader_pid}")
    leader_stream = stream
    leader_stream_lock.release()

current_operation = None
current_operation_lock = threading.Lock()

def set_current_operation(op):
    global current_operation, current_operation_lock
    current_operation_lock.acquire()
    current_operation = op
    current_operation_lock.release()

def reset_current_operation():
    set_current_operation(None)

resp_received = None
resp_received_lock = threading.Lock()

def set_resp_received(received):
    global resp_received, resp_received_lock
    resp_received_lock.acquire()
    resp_received = received
    resp_received_lock.release()
    
def server_communications(stream, stream_pid):
    global leader_pid, start_time

    addr = stream.getsockname()
    while True:
        if stream_pid != leader_pid:
            return

        try:
            data = stream.recv(1024)
        except socket.error as e:
            #decrement_leader_pid()
            connect_to_leader()
            break
        if not data:
            #decrement_leader_pid()
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
                # print(f'[RAW - {round(time.time() - start_time, 2)} - P{sender_pid}]: tokens = {payload_tokens}', flush=True)
                command = payload_tokens[0]
                if command == "leader":
                    leader_id = int(payload_tokens[1])
                    set_leader_pid(leader_id)
                    connect_to_leader()
                elif command == "resp":
                    value = payload_tokens[3]
                    if value == "None":
                        value = None
                    received_op = bc.Operation(payload_tokens[1],payload_tokens[2],value)
                    # print(f'[DEBUG - {round(time.time() - start_time, 2)} - P{sender_pid}]: {received_op.key} = {received_op.value}', flush=True)
                    if received_op == current_operation:
                        set_resp_received(True)
                        set_current_operation(None)
                        print(f'[{round(time.time() - start_time, 2)} - P{sender_pid}]: {received_op.key} = {received_op.value}\n', flush=True)
                else:
                    continue

def resend_payload(payload):
    global leader_pid, resp_received, leader_stream

    timeout = time.time() + TIMEOUT_LENGTH
    while timeout >= time.time():
        if resp_received:
            return
    
    log(f"no response from {leader_pid} after {TIMEOUT_LENGTH} seconds")
    decrement_leader_pid()
    connect_to_leader()
    log(f"resending {payload} to {leader_pid} with 'leader' command")
    direct_message(f"leader{PAYLOAD_DELIMITER}{payload}", leader_stream)
    threading.Thread(target=resend_payload, args=(payload,), daemon=True).start()
        

def input_listener():
    global leader_stream, current_operation, resp_received
    user_input = input()
    while True:
        if user_input == "leader_pid":
            log(f"I think the leader_pid is server {leader_pid}")
        if user_input == "state":
            log(f"Current State:\nCurrent Operation: {current_operation}\nResponse Received: {resp_received}")
        else:
            message = PAYLOAD_DELIMITER.join(user_input.split(" ", 2))
            try:
                message_tokens = message.split(PAYLOAD_DELIMITER)
                cmd = message_tokens[0]
                key = message_tokens[1]
                if cmd == "put":
                    value = message_tokens[2].strip("'")
                    set_current_operation(bc.Operation(cmd, key, value))
                    set_resp_received(False)
                    direct_message(message, leader_stream)
                    threading.Thread(target=resend_payload, args=(message,), daemon=True).start()
                elif cmd == "get":
                    set_current_operation(bc.Operation(cmd, key, None))
                    set_resp_received(False)
                    direct_message(message, leader_stream)
                    threading.Thread(target=resend_payload, args=(message,), daemon=True).start()
                else:
                    direct_message(message, leader_stream)
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
    global leader_pid, servers, max_pid

    if leader_pid == None:
        set_leader_pid(max_pid)

    cycle_start = leader_pid
    first_attempt = True

    while leader_pid != cycle_start or first_attempt:
        if first_attempt:
            first_attempt = False

        leader_stream = connect_to_server(leader_pid)
        if leader_stream != None:
            set_leader_stream(leader_stream)
            threading.Thread(target=server_communications,
            args=(leader_stream, leader_pid), daemon=True).start()
            threading.Thread(target=input_listener, args=(), daemon=True).start()
            return leader_stream
        decrement_leader_pid()
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
import sys
import os
import struct
import time

PAYLOAD_DELIMITER = " - "

def broadcast_message(message, streams, delay):
    time.sleep(delay)
    for s in streams:
        try:
            s.sendall(str.encode(message))
        except:
            print(f'failed to send {message} to {s.getsockname()}', flush=True)

def handle_exit(sockets):
    print(f'\nExiting program...', flush=True)
    sys.stdout.flush()
    for s in sockets:
        if s == None:
            continue
        print(f'Closed socket {s.getsockname()[1]}', flush=True)
        s.close()
    os._exit(0)

# connection flow = every processor sends out 2 requests and listens for 2
# p1 connects to p3 and p5, p1 listens for p4 and p2
# p3 connects to p5 and p4, p3 listens for p2 and p1
# p5 connects to p4 and p2, p5 listens for p1 and p3
# p4 connects to p2 and p1, p4 listens for p3 and p5
# p2 connects to p1 and p3, p2 listens for p4 and p5

def print_expecting_connections(pid, port_base):
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

def get_output_connection_tuples(pid, IP, port_base):
    if pid == 1:
        return (IP, port_base + 3), (IP, port_base + 5)
    elif pid == 2:
        return (IP, port_base + 1), (IP, port_base + 3)
    elif pid == 3:
        return (IP, port_base + 5), (IP, port_base + 4)
    elif pid == 4:
        return (IP, port_base + 2), (IP, port_base + 1)
    elif pid == 5:
        return (IP, port_base + 4), (IP, port_base + 2)
    
    raise Exception("pid must be 1, 2, 3, 4, or 5")

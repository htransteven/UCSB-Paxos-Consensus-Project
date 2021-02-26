import sys
import os

def handle_exit(sockets):
    print(f'\nExiting program...', flush=True)
    sys.stdout.flush()
    for s in sockets:
        print(f'Closed socket {s.getsockname()[1]}', flush=True)
        s.close()
    os._exit(0)
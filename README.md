# CS 171 Final Project

This project implements multi-paxos for consesus as well as a simplified blockchain model for record keeping.

## General Notes

CSV files are used for blockchain persistance and are un-tracked.

## Test Cases

### Basic Case 1

Normal operation with no failures

#### Setup

5 Servers, 1 Client, No network or hardware failures

Process:

- Startup P1, then P2, then P3, then P4, then P5, then Client
- put 1234 '{"phone": 8581234567 }'
  - Expected Behavior
    - P5 begins leader election, sends prepare, continues with Paxos
    - append new block to blockchain (and persist)
    - client receives 1234 = {"phone": 8581234567 }
- get 1234
  - Expected Behavior
    - P5 is already leader, no need for prepare, continues with Paxos
    - append new block to blockchain (and persist)
    - client receives 1234 = {"phone": 8581234567 }
- get bad_key
  - Expected Behavior
    - P5 is already leader, no need for prepare, continues with Paxos
    - append new block to blockchain (and persist)
    - client receives bad_key = NO_KEY

### Basic Case 2

Normal operation with failures

#### Setup

5 Servers, 1 Client, with 2 hardware failures, no network failures

Process:

- Startup P1, then P2, then P3, then P4, then P5, then Client
- Shutdown P5, Shutdown P4
- put 1234 '{"phone": 8581234567 }'
  - Expected Behavior
    - client sends to P5 but will timeout and try P3 (skips P4)
    - P3 begins leader election, sends prepare, continues with Paxos
    - append new block to blockchain (and persist)
    - client receives 1234 = {"phone": 8581234567 }
- get 1234
  - Expected Behavior
    - P3 is already leader, no need for prepare, continues with Paxos
    - append new block to blockchain (and persist)
    - client receives 1234 = {"phone": 8581234567 }
- get bad_key
  - Expected Behavior
    - P3 is already leader, no need for prepare, continues with Paxos
    - append new block to blockchain (and persist)
    - client receives bad_key = NO_KEY

### Basic Case 3

Normal operation with failures

#### Setup

5 Servers, 1 Client, with no hardware failures and 2 failed links from leader

Process:

- Startup P1, then P2, then P3, then P4, then P5, then Client
- Fail bi-directional link from P5 to P3 and from P5 to P1
- put 1234 '{"phone": 8581234567 }'
  - Expected Behavior
    - P5 begins leader election, sends prepare, continues with Paxos
    - P3 and P1 will NOT receive messages due to broken link, Paxos will carry on with P2 and P4
    - append new block to blockchain (and persist)
    - client receives 1234 = {"phone": 8581234567 }
- get 1234
  - Expected Behavior
    - P5 is already leader, no need for prepare, continues with Paxos
    - append new block to blockchain (and persist)
    - client receives 1234 = {"phone": 8581234567 }
- get bad_key
  - Expected Behavior
    - P5 is already leader, no need for prepare, continues with Paxos
    - append new block to blockchain (and persist)
    - client receives bad_key = NO_KEY

### Basic Case 4

No operation due to hardware failures

#### Setup

5 Servers, 1 Client, 3 hardware failures

Process:

- Startup P1, then P2, then P3, then P4, then P5, then Client
- put 1234 '{"phone": 8581234567 }'
  - Expected Behavior
    - P5 begins leader election, sends prepare, continues with Paxos
    - P3 and P1 will NOT receive messages due to broken link, Paxos will carry on with P2 and P4
    - append new block to blockchain (and persist)
    - client receives 1234 = {"phone": 8581234567 }
- Shutdown P5, P4 and P1
- get 1234
  - Expected Behavior
    - client sends to P5 but will timeout and try P3 (skips P4)
    - Only 2 servers are up (P3 and P2) so Paxos will not continue
    - client will timeout and send to P2
    - this will result in a timeout loop (client will terminate if it detects loop)

### Complex Case 1

Normal operation with failed paxos sequence after broadcast of "accept" messages

#### Setup

5 Servers, 1 Client, leader hardware failure

Process:

- Startup P1, then P2, then P3, then P4, then P5, then Client
- put 1234 '{"phone": 8581234567 }'
  - Expected Behavior
    - P5 begins leader election, sends prepare
    - Receives promises and sends "accept"
- After leader (P5) broadcasts "accept" messages, shutdown P5
  - Client will timeout and resend request to P4
- P4 will then receive the original command piggy-backed with a leader tag to force a new election
  - P4 will send prepares and recover the old value (same as proposed value)
  - P4 will carry on with paxos

### Complex Case 2

Normal operation with failed paxos sequence after broadcast of "accept" messages, and new client command from Client 2 to new leader

#### Setup

5 Servers, 2 Clients, leader hardware failure

Process:

- Startup P1, then P2, then P3, then P4, then P5, then Client 1
- From Client 1, send put 1234 '{"phone": 8581234567 }'
  - Expected Behavior
    - P5 begins leader election, sends prepare
    - Receives promises and sends "accept"
- After leader (P5) broadcasts "accept" messages, shutdown P5
  - Client 1 will timeout and resend request to P4
- Before Client 1 times out, Startup Client 2 (which will then connect to P4)
- From Client 2, send get 1234
- P4 will receive get 1234 before Client 1 times out
  - P4 will recover put 1234 '{"phone": 8581234567 }' and propose it
- P4 will then propose get 1234 after recovering

### Concurrency Case 1

Normal operation with higher server PID consuming both concurrent requests

#### Setup

5 Servers, 2 Clients, no failures

Process:

- Startup P1, then P2, then P3, then P4, then Client1, then P5, then Client 2
- (concurrent) From Client 1, send put 1234 '{"phone": 8581234567 }' to hinted leader(P4)
  - Expected Behavior
    - P4 begins leader election, sends prepare
    - Receives promises and loses to P5
- (concurrent) From Client 2, send put 1234 '{"phone": 1111111111 }' to hinted leader(P5)
  - Expected Behavior
    - P5 begins leader election, sends prepare
    - Receives promises and sends "accept"
    - wins election, stores forwarded payload from P4
- P4 executes put 1234 '{"phone": 1111111111 }', then put 1234 '{"phone": 8581234567 }'

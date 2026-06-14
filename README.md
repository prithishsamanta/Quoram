# Quorum: A Distributed Key-Value Store

Quorum is a distributed in-memory key-value store built from scratch in Java, implementing the **Raft consensus algorithm** for leader election and log replication across a multi-node cluster.

Every write is only acknowledged after being committed to a majority of nodes — strong consistency guaranteed.

---

## Tech Stack

- **Java 17+**
- **Raft Consensus** — implemented from scratch, no library
- **Raw TCP Sockets** — all inter-node and client communication
- **Jackson** — JSON message serialization
- **Maven** — build tool
- **Docker + Docker Compose** — containerized deployment *(Phase 6)*

---

## Project Structure

```
quorum/
├── src/main/java/com/quorum/
│   ├── Main.java                    — entry point, wires everything together
│   ├── server/
│   │   ├── RaftNode.java            — Raft state machine and consensus logic
│   │   ├── RaftState.java           — enum: FOLLOWER, CANDIDATE, LEADER
│   │   ├── LogEntry.java            — single log entry (index, term, command)
│   │   ├── RaftLog.java             — ordered replicated log
│   │   └── StateMachine.java        — ConcurrentHashMap, applies committed entries
│   ├── network/
│   │   ├── RaftServer.java          — TCP server, accepts all connections
│   │   ├── MessageHandler.java      — parses and routes incoming messages
│   │   └── PeerClient.java          — sends RPCs to peer nodes
│   ├── rpc/
│   │   ├── RequestVote.java         — RequestVote RPC request and response
│   │   └── AppendEntries.java       — AppendEntries RPC request and response
│   └── client/
│       └── ClientHandler.java       — handles one client connection per thread
└── pom.xml
```

---

## Prerequisites

- Java 17 or higher
- Maven 3.6 or higher
- netcat (`nc`) for testing — installed by default on Mac and Linux

Verify your setup:
```bash
java -version
mvn -version
nc -h
```

---

## Build

Clone the repository and build the fat JAR:

```bash
git clone https://github.com/prithishsamanta/Quorum.git
cd quorum
mvn clean package
```

This produces:
```
target/quorum-1.0-SNAPSHOT-jar-with-dependencies.jar
```

---

## Running A Single Node (Phase 1)

### Step 1: Start the node

Open a terminal and run:

```bash
java -jar target/quorum-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --id node1 \
  --port 8001
```

You should see:
```
[node1] Starting on port 8001
[Server] Listening on port 8001
```

### Step 2: Connect a client

Open a second terminal and connect with netcat:

```bash
nc localhost 8001
```

### Step 3: Send commands

Type each command and press Enter:

**PUT a key-value pair:**
```json
{"type":"CLIENT_PUT","key":"foo","value":"bar"}
```
Response:
```json
{"success":true,"type":"CLIENT_RESPONSE","message":"OK","value":null}
```

**GET a value:**
```json
{"type":"CLIENT_GET","key":"foo"}
```
Response:
```json
{"success":true,"type":"CLIENT_RESPONSE","message":"OK","value":"bar"}
```

**DELETE a key:**
```json
{"type":"CLIENT_DELETE","key":"foo"}
```
Response:
```json
{"success":true,"type":"CLIENT_RESPONSE","message":"OK","value":null}
```

**GET a deleted or missing key:**
```json
{"type":"CLIENT_GET","key":"foo"}
```
Response:
```json
{"success":false,"type":"CLIENT_RESPONSE","message":"Key not found","value":null}
```

---

## Running A Three-Node Cluster (Phase 3+)

### Step 1: Start three nodes

Open three separate terminals and run one command in each:

**Terminal 1: node1:**
```bash
java -jar target/quorum-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --id node1 \
  --port 8001 \
  --peers node2:8002,node3:8003
```

**Terminal 2: node2:**
```bash
java -jar target/quorum-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --id node2 \
  --port 8002 \
  --peers node1:8001,node3:8003
```

**Terminal 3: node3:**
```bash
java -jar target/quorum-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --id node3 \
  --port 8003 \
  --peers node1:8001,node2:8002
```

Within 300ms one node will win the election and become leader. You will see logs like:
```
[node2] Election timeout fired — becoming CANDIDATE
[node2] Won election for term 1 — now LEADER
[node1] Received heartbeat from node2 — resetting timer
[node3] Received heartbeat from node2 — resetting timer
```

### Step 2: Connect a client to any node

Open a fourth terminal and connect to any node — it does not matter which:

```bash
nc localhost 8001
```

### Step 3: Send commands

Commands work exactly the same as single node. If you connect to a follower, it automatically forwards writes to the leader.

```json
{"type":"CLIENT_PUT","key":"name","value":"prithish"}
{"type":"CLIENT_GET","key":"name"}
```

---

## Testing Fault Tolerance

This is the most impressive thing Quorum demonstrates.

### Step 1: Start three nodes and write some data

```bash
# Terminal 4 — client connected to node1
nc localhost 8001
{"type":"CLIENT_PUT","key":"foo","value":"bar"}
{"type":"CLIENT_PUT","key":"baz","value":"qux"}
```

### Step 2: Kill the leader

Find which node is the leader from the logs. Press **Ctrl+C** in that terminal to kill it.

### Step 3: Watch re-election happen

Within 300ms the remaining two nodes elect a new leader. You will see in their terminals:
```
[node1] Leader node2 stopped responding — starting election
[node1] Won election for term 2 — now LEADER
```

### Step 4: Verify data is intact

Send a GET from the client — still connected to its original node:
```json
{"type":"CLIENT_GET","key":"foo"}
```

Response:
```json
{"success":true,"type":"CLIENT_RESPONSE","message":"OK","value":"bar"}
```

Data is intact. Cluster is still serving requests. One node down, two nodes alive, majority maintained.

### Step 5: Restart the killed node

Run the same start command again in the killed terminal. The node rejoins as a follower, catches up with all missed log entries, and rejoins the cluster fully.

---

## Message Protocol

All messages are JSON over TCP with newline delimiter.

### Client → Node

| Type | Fields | Description |
|------|--------|-------------|
| `CLIENT_PUT` | `key`, `value` | Store a key-value pair |
| `CLIENT_GET` | `key` | Retrieve a value |
| `CLIENT_DELETE` | `key` | Delete a key |

### Node → Client

| Type | Fields | Description |
|------|--------|-------------|
| `CLIENT_RESPONSE` | `success`, `value`, `message` | Response to any client command |

### Node → Node (Raft RPCs)

| Type | Description |
|------|-------------|
| `REQUEST_VOTE` | Candidate requesting votes during election |
| `REQUEST_VOTE_RESPONSE` | Node's vote decision |
| `APPEND_ENTRIES` | Leader replicating log entries or sending heartbeat |
| `APPEND_ENTRIES_RESPONSE` | Follower acknowledging log entries |

---

## Key Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| Election timeout | 150-300ms randomized | Time before follower starts election |
| Heartbeat interval | 50ms | How often leader sends heartbeats |
| Minimum cluster size | 3 nodes | Required for meaningful fault tolerance |

---

## How Raft Works In Quorum

### Leader Election
Every node starts as a FOLLOWER with a randomized election timer. If no heartbeat arrives before the timer fires, the node becomes a CANDIDATE, increments its term, votes for itself, and requests votes from peers. The first candidate to receive votes from a majority becomes LEADER and immediately sends heartbeats to suppress new elections.

### Log Replication
Every client write goes to the leader. The leader appends the entry to its log and sends it to all followers via AppendEntries RPC. Once a majority of nodes acknowledge the entry, the leader marks it committed, applies it to the state machine, and responds success to the client.

### Strong Consistency
A write is only acknowledged after committing to a majority of nodes. If the leader crashes after committing, at least one surviving node has the entry and will carry it into future terms.

---

## Troubleshooting

**Port already in use:**
```bash
lsof -i :8001
kill -9 <PID>
```

**Build fails:**
```bash
mvn clean package -U
```
The `-U` flag forces dependency re-download.

**Netcat not available on Windows:**
Use telnet instead:
```bash
telnet localhost 8001
```
Or write a simple Java client using the QuorumClient class.

---

## What Is Not Implemented

These are intentionally out of scope for this version:

- Log compaction and snapshotting
- Dynamic cluster membership changes
- TLS encryption on inter-node communication
- Disk persistence (H2 coming in next version)
- Linearizable reads (reads currently served locally)

---

## Author

Prithish Samanta
[github.com/prithishsamanta](https://github.com/prithishsamanta)
[linkedin.com/in/prithish-samanta](https://linkedin.com/in/prithish-samanta)

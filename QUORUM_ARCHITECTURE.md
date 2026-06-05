# Quorum: A Distributed Key-Value Store
## Project Architecture, Requirements, and Phase Breakdown

---

## 1. Project Overview

Quorum is a distributed in-memory key-value store implementing the Raft consensus algorithm from scratch in Java. Multiple server nodes form a cluster, elect a leader, replicate every write across nodes, and handle node failures without losing data or availability. Clients connect to any node and get consistent results.

**Core guarantee:** A write is only acknowledged to the client after it has been committed to a majority of nodes in the cluster.

---

## 2. Tech Stack

```
Language:         Java 17+
Consensus:        Raft (implemented from scratch — no library)
Communication:    Raw Java TCP Sockets
Concurrency:      ExecutorService, ConcurrentHashMap, ScheduledExecutorService
Persistence:      H2 embedded database (added in Phase 6)
Containerization: Docker + Docker Compose (added in Phase 6)
Build Tool:       Maven
```

---

## 3. What We Are NOT Building

The following are intentionally out of scope to keep the project focused:

- Log compaction and snapshotting (Raft stretch goal)
- Dynamic cluster membership changes
- TLS or encryption on inter-node communication
- A web UI or REST API — TCP and CLI only
- Disk persistence before Phase 6

---

## 4. Project Structure

```
quorum/
├── src/main/java/com/quorum/
│   ├── server/
│   │   ├── RaftNode.java          — core Raft state machine and logic
│   │   ├── RaftState.java         — enum: FOLLOWER, CANDIDATE, LEADER
│   │   ├── LogEntry.java          — single log entry (index, term, command)
│   │   ├── RaftLog.java           — ordered list of log entries
│   │   └── StateMachine.java      — ConcurrentHashMap, applies committed entries
│   ├── network/
│   │   ├── RaftServer.java        — TCP server, accepts peer and client connections
│   │   ├── PeerClient.java        — TCP client, sends RPCs to peer nodes
│   │   └── MessageHandler.java    — parses and routes incoming messages
│   ├── rpc/
│   │   ├── RequestVote.java       — RequestVote RPC request and response
│   │   └── AppendEntries.java     — AppendEntries RPC request and response
│   ├── client/
│   │   └── ClientHandler.java     — handles client GET/PUT/DELETE over TCP
│   └── Main.java                  — entry point, parses args, starts node
├── src/main/resources/
│   └── node.properties            — node ID, port, peer addresses
├── Dockerfile                     — added in Phase 6
├── docker-compose.yml             — added in Phase 6
└── pom.xml
```

---

## 5. Core Classes — Detailed Specification

### 5.1 RaftState.java
```java
public enum RaftState {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
```

### 5.2 LogEntry.java
Every entry in the replicated log has exactly three fields:
```java
public class LogEntry {
    private final int index;       // position in log, starts at 1, never changes
    private final int term;        // election term when entry was created
    private final String command;  // "PUT key value" or "DELETE key"
}
```

### 5.3 RaftLog.java
Manages the ordered list of log entries. Must support:
- `append(LogEntry entry)` — add entry to end of log
- `getEntry(int index)` — retrieve entry by index
- `getLastIndex()` — index of last entry, 0 if empty
- `getLastTerm()` — term of last entry, 0 if empty
- `deleteFrom(int index)` — delete all entries from index onwards (conflict resolution)
- `getEntries(int fromIndex)` — get all entries from index onwards (for replication)

### 5.4 StateMachine.java
Wraps a ConcurrentHashMap. Applies committed log entries:
- `apply(LogEntry entry)` — executes PUT or DELETE on the map
- `get(String key)` — returns value for key, null if not found
- `getState()` — returns copy of entire map (for debugging)

In Phase 6, every `apply` call also writes through to H2.

### 5.5 RaftNode.java
The most complex class. Owns all Raft state and logic.

**Persistent state (must survive restarts — stored in H2 in Phase 6):**
```
currentTerm    int          — latest term node has seen, initialized to 0
votedFor       String       — candidateId voted for in current term, null if none
log            RaftLog      — the replicated log
```

**Volatile state (reset on restart):**
```
commitIndex    int          — index of highest log entry known to be committed
lastApplied    int          — index of highest log entry applied to state machine
state          RaftState    — current role: FOLLOWER, CANDIDATE, or LEADER
leaderId       String       — ID of current leader, null if unknown
```

**Leader-only volatile state (reset on election):**
```
nextIndex      Map<String, Integer>   — for each follower, index of next entry to send
matchIndex     Map<String, Integer>   — for each follower, highest index known replicated
```

**Timers owned by RaftNode:**
- Election timer — ScheduledExecutorService, randomized 150-300ms, resets on every heartbeat received
- Heartbeat timer — ScheduledExecutorService, fires every 50ms when node is LEADER

### 5.6 RequestVote.java
```java
// Request (Candidate → All Peers)
public class RequestVoteRequest {
    int term;           // candidate's current term
    String candidateId; // ID of candidate requesting vote
    int lastLogIndex;   // index of candidate's last log entry
    int lastLogTerm;    // term of candidate's last log entry
}

// Response (Peer → Candidate)
public class RequestVoteResponse {
    int term;           // current term of responding node (for candidate to update itself)
    boolean voteGranted; // true if candidate received the vote
}
```

### 5.7 AppendEntries.java
```java
// Request (Leader → All Followers) — also used as heartbeat when entries is empty
public class AppendEntriesRequest {
    int term;              // leader's current term
    String leaderId;       // so followers can redirect clients
    int prevLogIndex;      // index of log entry immediately preceding new ones
    int prevLogTerm;       // term of prevLogIndex entry
    List<LogEntry> entries; // entries to store, empty for heartbeat
    int leaderCommit;      // leader's commitIndex
}

// Response (Follower → Leader)
public class AppendEntriesResponse {
    int term;        // current term of responding node
    boolean success; // true if follower contained entry matching prevLogIndex and prevLogTerm
    String nodeId;   // so leader knows which follower responded
}
```

---

## 6. Message Protocol

All messages are JSON over TCP with newline delimiter (`\n`).

Every message has a `type` field that determines how it is parsed and routed.

```json
{"type":"REQUEST_VOTE","term":2,"candidateId":"node1","lastLogIndex":5,"lastLogTerm":1}\n
{"type":"REQUEST_VOTE_RESPONSE","term":2,"voteGranted":true}\n
{"type":"APPEND_ENTRIES","term":2,"leaderId":"node1","prevLogIndex":4,"prevLogTerm":1,"entries":[],"leaderCommit":4}\n
{"type":"APPEND_ENTRIES_RESPONSE","term":2,"success":true,"nodeId":"node2"}\n
{"type":"CLIENT_PUT","key":"foo","value":"bar"}\n
{"type":"CLIENT_GET","key":"foo"}\n
{"type":"CLIENT_DELETE","key":"foo"}\n
{"type":"CLIENT_RESPONSE","success":true,"value":"bar","message":""}\n
{"type":"FORWARD_TO_LEADER","originalMessage":"..."}\n
```

---

## 7. Threading Model

Each node runs the following threads:

| Thread | Count | Responsibility |
|--------|-------|----------------|
| Accept Thread | 1 | Accepts incoming TCP connections from peers and clients |
| Connection Handler | 1 per connection | Reads messages, dispatches to MessageHandler |
| Election Timer | 1 | Fires election timeout if no heartbeat received |
| Heartbeat Timer | 1 | Fires every 50ms when node is LEADER |
| Log Apply Thread | 1 | Watches commitIndex, applies newly committed entries to state machine |

Use a `ScheduledExecutorService` for election and heartbeat timers.
Use a fixed thread pool `ExecutorService` for connection handlers.
Use a single background thread for the log apply loop.

---

## 8. Key Configuration Numbers

These come directly from the Raft paper:

```
Election timeout:    randomized between 150ms and 300ms
Heartbeat interval:  50ms (must be significantly less than election timeout)
Minimum cluster:     3 nodes (required to test majority correctly)
Typical cluster:     3 or 5 nodes (odd numbers only)
```

---

## 9. Node Startup Configuration

Each node is started with command line arguments:

```bash
java -jar quorum.jar --id node1 --port 8001 --peers node2:8002,node3:8003
```

Or via a properties file:
```properties
node.id=node1
node.port=8001
peers=node2:8002,node3:8003
```

For local development, all three nodes run on localhost with different ports:
```
Node 1: localhost:8001
Node 2: localhost:8002
Node 3: localhost:8003
```

---

## 10. How Raft Works in This System — Full Flow

### 10.1 Startup and Election

```
1. All nodes start as FOLLOWER with currentTerm=0
2. Each node starts its election timer with a randomized timeout (150-300ms)
3. The first node whose timer fires becomes a CANDIDATE:
   - Increments currentTerm
   - Votes for itself
   - Sets votedFor = its own ID
   - Sends RequestVote RPC to all peers
4. Peers grant vote if:
   - They haven't voted in this term yet
   - Candidate's log is at least as up-to-date as their own
5. Candidate receiving votes from majority (2 out of 3) becomes LEADER
6. Leader immediately sends heartbeat AppendEntries to all followers
7. Followers reset their election timers on receiving heartbeat
```

### 10.2 Client Write (PUT or DELETE)

```
1. Client connects to any node and sends CLIENT_PUT or CLIENT_DELETE
2. If node is FOLLOWER:
   - Forwards request to leader using leaderId it knows
   - Leader handles from step 3
3. If node is LEADER:
   - Creates LogEntry {index: lastIndex+1, term: currentTerm, command: "PUT key value"}
   - Appends entry to its own log
   - Sends AppendEntries RPC to all followers in parallel
4. Each FOLLOWER receiving AppendEntries:
   - Verifies prevLogIndex and prevLogTerm match its own log
   - Appends new entries to its log
   - Responds with success=true
5. LEADER receives majority acknowledgments (itself + 1 follower = 2 out of 3):
   - Updates commitIndex to the new entry's index
   - Applies entry to its state machine (ConcurrentHashMap updated)
   - In Phase 6: writes to H2
   - Sends CLIENT_RESPONSE success to client
6. On next heartbeat, leader includes leaderCommit
7. Followers see leaderCommit > their commitIndex, apply entries up to leaderCommit
```

### 10.3 Client Read (GET)

```
1. Client connects to any node and sends CLIENT_GET
2. Node reads from its local ConcurrentHashMap
3. Returns value to client
Note: for strict linearizability reads should go through leader.
      Implement as a stretch goal after basic reads work.
```

### 10.4 Leader Failure and Re-election

```
1. Leader crashes — stops sending heartbeats
2. Followers' election timers fire (150-300ms after last heartbeat)
3. First follower to time out becomes CANDIDATE, starts new election
4. New leader elected with majority votes
5. New leader's log is guaranteed to contain all committed entries
   (Raft's election restriction ensures this)
6. Cluster continues operating with remaining nodes
```

---

## 11. Phase Breakdown

### Phase 1 — Single Node KV Server (4-6 hours)
**Goal:** A working single-node TCP server that accepts GET/PUT/DELETE commands from a CLI client.

**Build:**
- `Main.java` — parse args, start server
- `RaftServer.java` — TCP ServerSocket, accept connections, spawn handler threads
- `StateMachine.java` — ConcurrentHashMap, PUT/GET/DELETE operations
- `ClientHandler.java` — parse CLIENT_GET/PUT/DELETE messages, call StateMachine, return response
- `MessageHandler.java` — basic JSON parsing and routing

**Test:** Start one node, connect CLI client, PUT foo=bar, GET foo returns bar, DELETE foo, GET foo returns null.

**Do NOT move to Phase 2 until this works perfectly.**

---

### Phase 2 — Replicated Log (3-4 hours)
**Goal:** Add the write-ahead log structure. Every write appends to the log before applying to the state machine.

**Build:**
- `LogEntry.java` — index, term, command fields
- `RaftLog.java` — append, getEntry, getLastIndex, getLastTerm, deleteFrom, getEntries
- Wire `RaftLog` into `RaftNode`
- Every PUT/DELETE goes through log first, then applies to StateMachine

**Test:** PUT foo=bar creates LogEntry{index:1, term:0, command:"PUT foo=bar"}. Verify log grows correctly with multiple writes.

**Do NOT move to Phase 3 until log structure is solid.**

---

### Phase 3 — Leader Election (6-8 hours)
**Goal:** Three nodes elect a leader. Kill the leader, watch a new one get elected.

**Build:**
- `RaftNode.java` — full state machine (FOLLOWER/CANDIDATE/LEADER transitions)
- Election timer with randomized timeout (150-300ms)
- `RequestVote.java` — request and response classes
- `PeerClient.java` — TCP client to send RPCs to peers
- RequestVote RPC sending and receiving
- Vote counting and majority check
- Leader heartbeat timer (50ms interval)
- Heartbeat sending via empty AppendEntries

**Test:** 
- Start 3 nodes, one should become leader within 300ms
- Kill leader terminal, remaining two should elect new leader within 300ms
- Verify only one leader exists at any time

---

### Phase 4 — Log Replication (8-10 hours)
**Goal:** Leader replicates writes to followers. Write only acknowledged after majority commit.

**Build:**
- `AppendEntries.java` — request and response classes
- AppendEntries RPC sending from leader to all followers
- Follower log consistency check (prevLogIndex and prevLogTerm verification)
- Majority acknowledgment counting on leader
- commitIndex advancement after majority
- Log apply loop — applies committed entries to StateMachine
- leaderCommit propagation to followers via heartbeats

**Test:**
- PUT foo=bar on leader
- Verify all three nodes have identical logs
- Verify all three state machines have foo=bar
- Kill one follower, do more writes, restart follower, verify it catches up

---

### Phase 5 — Wire Everything Together (4-6 hours)
**Goal:** Client can connect to any node. Fault tolerance works end to end.

**Build:**
- Follower request forwarding — follower receives client write, forwards to leader
- Client receives transparent response regardless of which node it connected to
- Graceful handling of leader changes during forwarding

**Test (the full demo sequence):**
```
1. Start 3 nodes — node1 becomes leader
2. PUT foo=bar via node2 (follower) — succeeds, forwarded to leader
3. GET foo via node3 — returns bar
4. Kill node1 (leader)
5. node2 or node3 becomes new leader
6. PUT baz=qux — succeeds
7. GET foo — still returns bar (data preserved)
8. Restart node1 — it rejoins as follower, catches up
9. GET foo on node1 — returns bar
```

---

### Phase 6 — H2 Persistence + Docker + README (4-6 hours)
**Goal:** Persistent storage, containerized deployment, demo recording, clean README.

**Build — H2 Persistence:**
- Add H2 dependency to pom.xml
- Create schema: `kv_store` table and `raft_metadata` table
- Modify StateMachine.apply() to write through to H2
- Add startup recovery — load H2 state into ConcurrentHashMap on node start
- Persist currentTerm and votedFor to H2 (required by Raft paper for correctness)

**Build — Docker:**
- `Dockerfile` — build JAR, create image
- `docker-compose.yml` — three services with named volumes for H2 persistence

**Build — README:**
- Architecture diagram showing nodes, Raft roles, client interaction
- One-line description of what Quorum is and what problem it solves
- Cluster spin-up instructions — working cluster in under 5 commands
- Demo terminal recording showing: write, node failure, re-election, data intact
- Implementation notes — interesting decisions you made
- What you would add next — log compaction, disk persistence, dynamic membership

---

## 12. H2 Schema (Phase 6)

```sql
CREATE TABLE IF NOT EXISTS kv_store (
    key   VARCHAR(255) PRIMARY KEY,
    value VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS raft_metadata (
    node_id      VARCHAR(50)  PRIMARY KEY,
    current_term INT          NOT NULL DEFAULT 0,
    voted_for    VARCHAR(50),
    last_applied INT          NOT NULL DEFAULT 0
);
```

Each node has its own H2 instance. They do not share a database.

```
node1 → ConcurrentHashMap → H2 (/data/node1.db)
node2 → ConcurrentHashMap → H2 (/data/node2.db)
node3 → ConcurrentHashMap → H2 (/data/node3.db)
```

---

## 13. Docker Compose (Phase 6)

```yaml
services:
  node1:
    build: .
    ports:
      - "8001:8001"
    volumes:
      - node1-data:/data
    environment:
      - DB_PATH=/data/node1.db
    command: ["--id", "node1", "--port", "8001", "--peers", "node2:8002,node3:8003"]

  node2:
    build: .
    ports:
      - "8002:8002"
    volumes:
      - node2-data:/data
    environment:
      - DB_PATH=/data/node2.db
    command: ["--id", "node2", "--port", "8002", "--peers", "node1:8001,node3:8003"]

  node3:
    build: .
    ports:
      - "8003:8003"
    volumes:
      - node3-data:/data
    environment:
      - DB_PATH=/data/node3.db
    command: ["--id", "node3", "--port", "8003", "--peers", "node1:8001,node2:8002"]

volumes:
  node1-data:
  node2-data:
  node3-data:
```

---

## 14. Local Development Setup

**Running locally (Phases 1-5):**

Open three terminals and run:
```bash
# Terminal 1
java -jar quorum.jar --id node1 --port 8001 --peers node2:8002,node3:8003

# Terminal 2
java -jar quorum.jar --id node2 --port 8002 --peers node1:8001,node3:8003

# Terminal 3
java -jar quorum.jar --id node3 --port 8003 --peers node1:8001,node2:8002
```

To simulate node failure: Ctrl+C on any terminal.
To simulate node recovery: restart the same command.

**Verify ports are free before starting:**
```bash
lsof -i :8001
lsof -i :8002
lsof -i :8003
```

---

## 15. Resume Bullets (Final)

```
Implemented the Raft consensus algorithm from scratch in Java across a multi-node
cluster, handling leader election with randomized election timeouts, log replication
with majority acknowledgment, and automatic failover so the cluster remains available
as long as a majority of nodes are online.

Built the inter-node communication layer over raw Java TCP sockets with a custom
binary protocol for vote requests, heartbeats, and log replication, managing concurrent
connections via ExecutorService and handling network partitions and delayed messages
without corrupting cluster state.

Exposed a client-facing TCP API supporting GET, PUT, and DELETE with strong consistency,
where writes are only acknowledged after committing to a majority of nodes and
automatically forwarded to the leader if a client connects to a follower.
```

---

## 16. Important Rules While Building

1. **Never skip a phase.** Do not start Phase 3 until Phase 2 tests pass completely.
2. **Test after every sub-component.** Do not write 500 lines and then test.
3. **Log everything.** Every state transition, every RPC sent and received, every election. You will need these logs to debug.
4. **One leader at a time.** If you ever see two nodes both believing they are leader, you have a bug in election logic. Stop and fix it before continuing.
5. **The Raft paper is your specification.** When in doubt about behavior, read Section 5 of the paper. It is authoritative.

---

*Document version 1.0 — June 2026*
*Project: Quorum — Distributed Key-Value Store*
*Author: Prithish Samanta*

package com.quorum.server;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Random;
import java.util.concurrent.ScheduledFuture;
import java.util.ArrayList;

import com.quorum.network.NodeCommunicator;
import com.quorum.rpc.AppendEntries;
import com.quorum.rpc.RequestVote;

public class RaftNode {
    private final String id;
    private final List<String> peers;
    private final StateMachine stateMachine;
    private final RaftLog raftLog;

    private RaftState state;
    private int currentTerm;
    private String votedFor;

    private NodeCommunicator nodeCommunicator; // sends RPCs to the peers
    private final ScheduledExecutorService electionTimer; // triggers elections
    private final ScheduledExecutorService heartbeatTimer; // sends heartbeats
    private final ScheduledExecutorService statusPrinter;
    private int commitIndex; // highest committed log index
    private int lastApplied; // highest applied log index
    private Map<String, Integer> nextIndex; // leader only, per follower
    private Map<String, Integer> matchIndex; // leader only, per follower
    private int votesReceived; // tracks votes during elections
    private ScheduledFuture<?> currentElectionTask;
    private ScheduledFuture<?> currentHeartbeatTask;

    public RaftNode(String id, List<String> peers, StateMachine stateMachine) {
        this.id = id;
        this.peers = peers;
        this.stateMachine = stateMachine;
        this.raftLog = new RaftLog();
        this.nodeCommunicator = new NodeCommunicator();
    
        this.state = RaftState.FOLLOWER;
        this.currentTerm = 0;
        this.votedFor = null;
        this.votesReceived = 0;
    
        this.commitIndex = 0;
        this.lastApplied = 0;
        this.nextIndex = new ConcurrentHashMap<>();
        this.matchIndex = new ConcurrentHashMap<>();
    
        this.electionTimer = Executors.newSingleThreadScheduledExecutor();
        this.heartbeatTimer = Executors.newSingleThreadScheduledExecutor();
        this.statusPrinter = Executors.newSingleThreadScheduledExecutor();
    
        // Start the election timer immediately
        resetElectionTimer();

        // Print a clean status line every 2 seconds for easy testing
        statusPrinter.scheduleAtFixedRate(() -> {
            System.out.println("[" + id + "] STATUS — state=" + state + " term=" + currentTerm);
        }, 2, 2, TimeUnit.SECONDS);
    }
    
    public String handleGet(String key) {
        return stateMachine.get(key);
    }

    public void handlePut(String key, String value) {
        String command = "PUT " + key + " " + value;
        int newIndex = raftLog.getLastIndex() + 1;
        LogEntry entry = new LogEntry(newIndex, currentTerm, command);
        raftLog.append(entry);
        stateMachine.apply(entry);
        System.out.println("[" + id + "] Appended to log: " + entry);
    }

    public void handleDelete(String key) {
        String command = "DELETE " + key;
        int newIndex = raftLog.getLastIndex() + 1;
        LogEntry entry = new LogEntry(newIndex, currentTerm, command);
        raftLog.append(entry);
        stateMachine.apply(entry);
        System.out.println("[" + id + "] Appended to log: " + entry);
    }

    // decide whether to grant vote
    public synchronized RequestVote.Response handleRequestVote(RequestVote.Request request){
        // Rule 1: if request term > our term, update our term and become follower
        if (request.term > currentTerm) {
            becomeFollower(request.term);
        }
        
        // Rule 2: reject if request term < our term
        if (request.term < currentTerm) {
            return new RequestVote.Response(currentTerm, false);
        }
        
        // Rule 3: check if we already voted for someone else this term
        boolean canVote = (votedFor == null || votedFor.equals(request.candidateId));
        
        // Rule 4: check if candidate log is at least as up to date as ours
        boolean logOk = (request.lastLogTerm > raftLog.getLastTerm()) ||
                        (request.lastLogTerm == raftLog.getLastTerm() && 
                        request.lastLogIndex >= raftLog.getLastIndex());
        
        if (canVote && logOk) {
            votedFor = request.candidateId;
            resetElectionTimer();
            return new RequestVote.Response(currentTerm, true);
        }
        
        return new RequestVote.Response(currentTerm, false);
    }

    // process heartbeat or log entries from leader
    public synchronized AppendEntries.Response handleAppendEntries(AppendEntries.Request request) {
        // reject if term is old
        if (request.term < currentTerm) {
            return new AppendEntries.Response(currentTerm, false, id);
        }
    
        //  valid leader, update term and become follower
        if (state != RaftState.FOLLOWER || request.term > currentTerm) {
            becomeFollower(request.term);
        }
    
        // reset election timer if leader is alive
        resetElectionTimer();
    
        return new AppendEntries.Response(currentTerm, true, id);
    }

    public String getId() { return id; }
    public List<String> getPeers() { return peers; }
    public RaftState getState() { return state; }
    public int getCurrentTerm() { return currentTerm; }
    public String getVotedFor() { return votedFor; }
    public RaftLog getRaftLog() { return raftLog; }

    // become candidate, request votes
    public synchronized void startElection(){
        // 1. Become candidate
        state = RaftState.CANDIDATE;
        
        // 2. Increment term
        currentTerm++;
        
        // 3. Vote for yourself
        votedFor = id;
        votesReceived = 1;
        
        // 4. Reset election timer in case nobody wins
        resetElectionTimer();
        
        // 5. Send RequestVote to all peers
        for (String peer : peers) {
            new Thread(() -> {
                RequestVote.Request request = new RequestVote.Request(
                    currentTerm,
                    id,
                    raftLog.getLastIndex(),
                    raftLog.getLastTerm()
                );
        
                RequestVote.Response response = nodeCommunicator.sendRequestVote(peer, request);
        
                if (response == null) {
                    return; // peer unreachable, skip
                }
        
                synchronized (this) {
                    if (response.term > currentTerm) {
                        becomeFollower(response.term);
                        return;
                    }
        
                    if (state == RaftState.CANDIDATE && response.voteGranted) {
                        votesReceived++;
                        int majority = (peers.size() + 1) / 2 + 1;
                        if (votesReceived >= majority) {
                            becomeLeader();
                        }
                    }
                }
            }).start();
        }
    }

    // transition to leader, start heartbeats
    private synchronized void becomeLeader() {
        state = RaftState.LEADER;
        System.out.println("[" + id + "] Won election for term " + currentTerm + " — now LEADER");
    
        // A leader does not need an election timer — cancel any pending one
        if (currentElectionTask != null) {
            currentElectionTask.cancel(false);
        }
    
        for (String peer : peers) {
            nextIndex.put(peer, raftLog.getLastIndex() + 1);
            matchIndex.put(peer, 0);
        }
    
        startHeartbeatTimer();
    }

    // transition to follower, reset timer
    private synchronized void becomeFollower(int term) {
        state = RaftState.FOLLOWER;
        currentTerm = term;
        votedFor = null;
        System.out.println("[" + id + "] Becoming FOLLOWER for term " + term);
    
        stopHeartbeatTimer();
        resetElectionTimer();
    }

    // cancel and reschedule election timer
    // Schedule with random delay between 150-300ms
    public void resetElectionTimer(){
        // cancel existing timer
        // schedule new one with random timeout
        // when it fires → call startElection()
        if(currentElectionTask != null){
            currentElectionTask.cancel(false);
        }

        int timeout = 150 + new Random().nextInt(150);

        // Schedule a new one time task
        currentElectionTask = electionTimer.schedule(
            this::startElection,
            timeout,
            TimeUnit.MILLISECONDS
        );
    }

    // leader sends empty AppendEntries to all peers
    private void sendHeartbeats() {
        // Only leader sends heartbeats
        if (state != RaftState.LEADER) {
            return;
        }
    
        for (String peer : peers) {
            // Run each peer's heartbeat on its own thread
            new Thread(() -> {
                AppendEntries.Request request = new AppendEntries.Request(
                    currentTerm, // tells follower what term the leader is in
                    id, // tells follower who the leader is 
                    raftLog.getLastIndex(), // leader's last log index, lets follower verify it is in sync
                    raftLog.getLastTerm(), // leader's last log term
                    new ArrayList<>(),  // empty entries, heartbeat only in Phase 3
                    commitIndex // leader's commitIndex, tells follower what is safe to apply
                );
    
                AppendEntries.Response response = nodeCommunicator.sendAppendEntries(peer, request);
    
                if (response != null && response.term > currentTerm) {
                    // Peer has a higher term — we are no longer leader
                    becomeFollower(response.term);
                }
            }).start();
        }
    }

    private void startHeartbeatTimer() {
        currentHeartbeatTask = heartbeatTimer.scheduleAtFixedRate(
            this::sendHeartbeats,   // what to run
            0,                       // initial delay
            50,                      // repeat every 50ms
            TimeUnit.MILLISECONDS
        );
    }
    
    private void stopHeartbeatTimer() {
        if (currentHeartbeatTask != null) {
            currentHeartbeatTask.cancel(false);
        }
    }
}
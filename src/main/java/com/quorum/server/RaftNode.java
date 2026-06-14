package com.quorum.server;

import java.util.List;

public class RaftNode {
    private final String id;
    private final List<String> peers;
    private final StateMachine stateMachine;
    private final RaftLog raftLog;

    private RaftState state;
    private int currentTerm;
    private String votedFor;

    public RaftNode(String id, List<String> peers, StateMachine stateMachine) {
        this.id = id;
        this.peers = peers;
        this.stateMachine = stateMachine;
        this.raftLog = new RaftLog();

        this.state = RaftState.FOLLOWER;
        this.currentTerm = 0;
        this.votedFor = null;
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

    public String getId() { return id; }
    public List<String> getPeers() { return peers; }
    public RaftState getState() { return state; }
    public int getCurrentTerm() { return currentTerm; }
    public String getVotedFor() { return votedFor; }
    public RaftLog getRaftLog() { return raftLog; }
}
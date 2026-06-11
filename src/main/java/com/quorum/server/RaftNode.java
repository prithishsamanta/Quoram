package com.quorum.server;

import java.util.List;

public class RaftNode {
    private final String id;
    private final List<String> peers;
    private final StateMachine stateMachine;

    private RaftState state;
    private int currentTerm;
    private String votedFor;
    
    public RaftNode(String id, List<String> peers, StateMachine stateMachine) {
        this.id = id;
        this.peers = peers;
        this.stateMachine = stateMachine;

        // Every node starts as a follower
        this.state = RaftState.FOLLOWER;
        this.currentTerm = 0;
        this.votedFor = null;
    }

    public String handleGet(String key) {
        return stateMachine.get(key);
    }

    public void handlePut(String key, String value) {
        // Phase 1: direct to StateMachine
        // Phase 4: will append to log, replicate, wait for majority
        stateMachine.put(key, value);
    }

    public void handleDelete(String key) {
        // Phase 1: direct to StateMachine
        // Phase 4: will append to log, replicate, wait for majority
        stateMachine.delete(key);
    }

    public String getId() { 
        return id; 
    }
    public List<String> getPeers() { 
        return peers; 
    }
    public RaftState getState() { 
        return state; 
    }
    public int getCurrentTerm() { 
        return currentTerm; 
    }
    public String getVotedFor() { 
        return votedFor; 
    }
}

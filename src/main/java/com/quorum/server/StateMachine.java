package com.quorum.server;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class StateMachine {
    private final ConcurrentHashMap<String, String> store;

    public StateMachine() {
        this.store = new ConcurrentHashMap<>();
    }

    // apply a committed log entry
    public void apply(LogEntry entry){
        String command = entry.getCommand();
        String[] parts = command.split(" ");
        if (parts[0].equals("PUT") && parts.length == 3) {
            put(parts[1], parts[2]);
        } else if (parts[0].equals("DELETE") && parts.length == 2) {
            delete(parts[1]);
        } else {
            throw new IllegalArgumentException("Invalid command: " + command);
        }
    }

    // Called when a PUT command is committed
    public void put(String key, String value){
        store.put(key, value);
    }

    // Called when a DELETE command is committed  
    public void delete(String key){
        store.remove(key);
    }

    // direct read for client GET
    public String get(String key){
        return store.get(key);
    }

    // for debugging - return full state
    public ConcurrentHashMap<String, String> getState(){
        return new ConcurrentHashMap<>(store);
    }
    
}

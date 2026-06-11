package com.quorum.server;

public class LogEntry {
    private final int index;
    private final int term;
    private final String command;

    public LogEntry(int index, int term, String command) {
        this.index = index;
        this.term = term;
        this.command = command;
    }

    public int getIndex() {
        return index;
    }

    public int getTerm() {
        return term;
    }
    
    public String getCommand() {
        return command;
    }
}

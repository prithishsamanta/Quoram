package com.quorum.server;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;


public class RaftLog {
    private final List<LogEntry> entries;

    public RaftLog() {
        this.entries = new ArrayList<>();
    }

    public void append(LogEntry entry) {
        entries.add(entry);
    }

    public synchronized LogEntry getEntry(int index) {
        if (index < 1 || index > entries.size()) {
            return null;
        }
        return entries.get(index - 1); // log index 1 = ArrayList position 0
    }

    public int getLastIndex() {
        return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).getIndex();
    }
    
    public int getLastTerm() {
        return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).getTerm();
    }

    public void deleteFrom(int index) {
        entries.subList(index, entries.size()).clear();
    }

    public List<LogEntry> getEntries(int fromIndex) {
        return entries.subList(fromIndex, entries.size());
    }
}

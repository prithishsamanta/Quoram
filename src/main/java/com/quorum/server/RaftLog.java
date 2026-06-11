package com.quorum.server;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.quorum.server.LogEntry;

public class RaftLog {
    private final CopyOnWriteArrayList<LogEntry> entries;

    public RaftLog() {
        this.entries = new CopyOnWriteArrayList<>();
    }

    public void append(LogEntry entry) {
        entries.add(entry);
    }

    public LogEntry getEntry(int index) {
        return entries.get(index);
    }

    public int getLastIndex() {
        return entries.size() - 1;
    }

    public int getLastTerm() {
        return entries.get(entries.size() - 1).getTerm();
    }

    public void deleteFrom(int index) {
        entries.subList(index, entries.size()).clear();
    }

    public List<LogEntry> getEntries(int fromIndex) {
        return entries.subList(fromIndex, entries.size());
    }
}

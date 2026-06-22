package com.quorum.rpc;

// Used for heartbeats and log replication
import com.quorum.server.LogEntry;
import java.util.List;

public class AppendEntries {

    public static class Request {
        public int term;
        public String leaderId;
        public int prevLogIndex;
        public int prevLogTerm;
        public List<LogEntry> entries;
        public int leaderCommit;

        public Request() {}

        public Request(int term, String leaderId, int prevLogIndex,
                       int prevLogTerm, List<LogEntry> entries, int leaderCommit) {
            this.term = term;
            this.leaderId = leaderId;
            this.prevLogIndex = prevLogIndex;
            this.prevLogTerm = prevLogTerm;
            this.entries = entries;
            this.leaderCommit = leaderCommit;
        }

        @Override
        public String toString() {
            return "AppendEntries.Request{term=" + term + ", leaderId='" + leaderId +
                   "', prevLogIndex=" + prevLogIndex + ", entries=" +
                   (entries != null ? entries.size() : 0) + "}";
        }
    }

    public static class Response {
        public int term;
        public boolean success;
        public String nodeId;

        public Response() {}

        public Response(int term, boolean success, String nodeId) {
            this.term = term;
            this.success = success;
            this.nodeId = nodeId;
        }

        @Override
        public String toString() {
            return "AppendEntries.Response{term=" + term + ", success=" + success +
                   ", nodeId='" + nodeId + "'}";
        }
    }
}

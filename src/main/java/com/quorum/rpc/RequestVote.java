package com.quorum.rpc;

// Uwed during elections
public class RequestVote {

    public static class Request {
        public int term;
        public String candidateId;
        public int lastLogIndex;
        public int lastLogTerm;

        public Request() {}

        public Request(int term, String candidateId, int lastLogIndex, int lastLogTerm) {
            this.term = term;
            this.candidateId = candidateId;
            this.lastLogIndex = lastLogIndex;
            this.lastLogTerm = lastLogTerm;
        }

        @Override
        public String toString() {
            return "RequestVote.Request{term=" + term + ", candidateId='" + candidateId + 
                   "', lastLogIndex=" + lastLogIndex + ", lastLogTerm=" + lastLogTerm + "}";
        }
    }

    public static class Response {
        public int term;
        public boolean voteGranted;

        public Response() {}

        public Response(int term, boolean voteGranted) {
            this.term = term;
            this.voteGranted = voteGranted;
        }

        @Override
        public String toString() {
            return "RequestVote.Response{term=" + term + ", voteGranted=" + voteGranted + "}";
        }
    }
}
package com.quorum.network;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quorum.rpc.AppendEntries;
import com.quorum.rpc.RequestVote;
import com.quorum.server.LogEntry;
import com.quorum.server.RaftNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageHandler {
    // Takes raw JSON string
    // Reads the "type" field
    // Routes to correct handler method

    private final RaftNode node;
    private final ObjectMapper mapper;

    public MessageHandler(RaftNode node) {
        this.node = node;
        this.mapper = new ObjectMapper();
    }

    public String handle(String rawMessage) {
        try {
            // Parse raw JSON string into a map
            Map<String, Object> message = mapper.readValue(rawMessage, Map.class);
            String type = (String) message.get("type");

            if (type == null) {
                return buildResponse(false, null, "Missing type field");
            }

            switch (type) {
                case "CLIENT_GET":
                    return handleGet(message);
                case "CLIENT_PUT":
                    return handlePut(message);
                case "CLIENT_DELETE":
                    return handleDelete(message);
                case "REQUEST_VOTE":
                    return handleRequestVote(message);
                case "APPEND_ENTRIES":
                    return handleAppendEntries(message);
                default:
                    return buildResponse(false, null, "Unknown type: " + type);
            }

        } catch (Exception e) {
            return buildResponse(false, null, "Failed to parse message: " + e.getMessage());
        }
    }

    private String handleGet(Map<String, Object> message) {
        String key = (String) message.get("key");
        if (key == null) {
            return buildResponse(false, null, "Missing key");
        }
        String value = node.handleGet(key);
        if (value == null) {
            return buildResponse(false, null, "Key not found");
        }
        return buildResponse(true, value, "OK");
    }

    private String handlePut(Map<String, Object> message) {
        String key = (String) message.get("key");
        String value = (String) message.get("value");
        if (key == null || value == null) {
            return buildResponse(false, null, "Missing key or value");
        }
        node.handlePut(key, value);
        return buildResponse(true, null, "OK");
    }

    private String handleDelete(Map<String, Object> message) {
        String key = (String) message.get("key");
        if (key == null) {
            return buildResponse(false, null, "Missing key");
        }
        node.handleDelete(key);
        return buildResponse(true, null, "OK");
    }

    private String handleRequestVote(Map<String, Object> message) throws Exception {
        int term = (Integer) message.get("term");
        String candidateId = (String) message.get("candidateId");
        int lastLogIndex = (Integer) message.get("lastLogIndex");
        int lastLogTerm = (Integer) message.get("lastLogTerm");
    
        RequestVote.Request request = new RequestVote.Request(term, candidateId, lastLogIndex, lastLogTerm);
        RequestVote.Response response = node.handleRequestVote(request);
    
        Map<String, Object> responseMap = new HashMap<>();
        responseMap.put("type", "REQUEST_VOTE_RESPONSE");
        responseMap.put("term", response.term);
        responseMap.put("voteGranted", response.voteGranted);
        return mapper.writeValueAsString(responseMap);
    }

    private String handleAppendEntries(Map<String, Object> message) throws Exception {
        int term = (Integer) message.get("term");
        String leaderId = (String) message.get("leaderId");
        int prevLogIndex = (Integer) message.get("prevLogIndex");
        int prevLogTerm = (Integer) message.get("prevLogTerm");
        int leaderCommit = (Integer) message.get("leaderCommit");
    
        // entries will be empty in Phase 3 but parse it properly anyway
        List<LogEntry> entries = new ArrayList<>(); // Phase 3: always empty
    
        AppendEntries.Request request = new AppendEntries.Request(
            term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit
        );
        AppendEntries.Response response = node.handleAppendEntries(request);
    
        Map<String, Object> responseMap = new HashMap<>();
        responseMap.put("type", "APPEND_ENTRIES_RESPONSE");
        responseMap.put("term", response.term);
        responseMap.put("success", response.success);
        responseMap.put("nodeId", response.nodeId);
        return mapper.writeValueAsString(responseMap);
    }

    // Builds a consistent JSON response every time
    private String buildResponse(boolean success, String value, String message) {
        Map<String, Object> response = new HashMap<>();
        response.put("type", "CLIENT_RESPONSE");
        response.put("success", success);
        response.put("value", value);
        response.put("message", message);
        try {
            return mapper.writeValueAsString(response);
        } catch (JsonProcessingException e) {
            return "{\"type\":\"CLIENT_RESPONSE\",\"success\":false,\"message\":\"Failed to serialize response\"}";
        }
    }
}

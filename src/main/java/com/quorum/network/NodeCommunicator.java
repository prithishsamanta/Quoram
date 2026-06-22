package com.quorum.network;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.quorum.rpc.AppendEntries;
import com.quorum.rpc.RequestVote;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NodeCommunicator {
    private final ObjectMapper mapper;

    // One persistent connection per peer, reused across calls instead of
    // opening a brand new socket on every single heartbeat/RPC.
    // Opening a new socket every 50ms exhausts ephemeral ports quickly
    // (each closed socket sits in TIME_WAIT for ~60s), which was causing
    // the cascading election failures seen earlier.
    private final Map<String, Socket> sockets = new ConcurrentHashMap<>();
    private final Map<String, BufferedReader> readers = new ConcurrentHashMap<>();
    private final Map<String, PrintWriter> writers = new ConcurrentHashMap<>();

    public NodeCommunicator() {
        this.mapper = new ObjectMapper();
    }

    public RequestVote.Response sendRequestVote(String peer, RequestVote.Request request) {
        try {
            // ensure we have a live connection to this peer (reuse if possible)
            ensureConnected(peer);

            Map<String, Object> message = new HashMap<>();
            message.put("type", "REQUEST_VOTE");
            message.put("term", request.term);
            message.put("candidateId", request.candidateId);
            message.put("lastLogIndex", request.lastLogIndex);
            message.put("lastLogTerm", request.lastLogTerm);

            writers.get(peer).println(mapper.writeValueAsString(message));

            // read response
            String responseJson = readers.get(peer).readLine();
            if (responseJson == null) {
                // peer closed the connection — discard it so next call reconnects
                closeConnection(peer);
                return null;
            }

            // parse and return response
            Map<String, Object> responseMap = mapper.readValue(responseJson, Map.class);
            int term = (Integer) responseMap.get("term");
            boolean voteGranted = (Boolean) responseMap.get("voteGranted");

            return new RequestVote.Response(term, voteGranted);

        } catch (Exception e) {
            System.err.println("[NodeCommunicator] Failed to reach " + peer + ": " + e.getMessage());
            // connection is likely broken — discard it so next call reconnects fresh
            closeConnection(peer);
            return null;
        }
    }

    public AppendEntries.Response sendAppendEntries(String peer, AppendEntries.Request request) {
        try {
            // ensure we have a live connection to this peer (reuse if possible)
            ensureConnected(peer);

            Map<String, Object> message = new HashMap<>();
            message.put("type", "APPEND_ENTRIES");
            message.put("term", request.term);
            message.put("leaderId", request.leaderId);
            message.put("prevLogIndex", request.prevLogIndex);
            message.put("prevLogTerm", request.prevLogTerm);
            message.put("entries", request.entries);
            message.put("leaderCommit", request.leaderCommit);

            writers.get(peer).println(mapper.writeValueAsString(message));

            String responseJson = readers.get(peer).readLine();
            if (responseJson == null) {
                // peer closed the connection — discard it so next call reconnects
                closeConnection(peer);
                return null;
            }

            Map<String, Object> responseMap = mapper.readValue(responseJson, Map.class);
            int term = (Integer) responseMap.get("term");
            boolean success = (Boolean) responseMap.get("success");
            String nodeId = (String) responseMap.get("nodeId");

            return new AppendEntries.Response(term, success, nodeId);
        } catch (Exception e) {
            System.err.println("[NodeCommunicator] Failed to reach " + peer
                + ": " + e.getMessage());
            // connection is likely broken — discard it so next call reconnects fresh
            closeConnection(peer);
            return null;
        }
    }

    // Opens a new socket to the peer only if one doesn't already exist
    // or the existing one has died. Otherwise reuses the existing connection.
    private void ensureConnected(String peer) throws Exception {
        Socket socket = sockets.get(peer);
        if (socket != null && socket.isConnected() && !socket.isClosed()) {
            return; // already connected, reuse it
        }

        // parse peer address
        String host = getHost(peer);
        int port = getPort(peer);

        // open socket to peer (with a connect timeout so dead peers fail fast)
        Socket newSocket = new Socket();
        newSocket.connect(new InetSocketAddress(host, port), 1000);
        newSocket.setSoTimeout(500); // read timeout per RPC call

        // create reader and writer, store them for reuse on future calls
        sockets.put(peer, newSocket);
        readers.put(peer, new BufferedReader(new InputStreamReader(newSocket.getInputStream())));
        writers.put(peer, new PrintWriter(newSocket.getOutputStream(), true));
    }

    // Closes and removes a broken connection so the next call reconnects fresh
    private void closeConnection(String peer) {
        try {
            Socket socket = sockets.remove(peer);
            if (socket != null) {
                socket.close();
            }
        } catch (Exception e) {
            System.err.println("[NodeCommunicator] Error closing socket: " + e.getMessage());
        }
        readers.remove(peer);
        writers.remove(peer);
    }

    private String getHost(String peer){
        String[] peers = peer.split(":");
        return peers[0];
    }

    private int getPort(String peer){
        String[] peers = peer.split(":");
        return Integer.parseInt(peers[1]);
    }
}
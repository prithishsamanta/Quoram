package com.quorum;

import com.quorum.network.RaftServer;
import com.quorum.server.RaftNode;
import com.quorum.server.StateMachine;
import com.quorum.network.MessageHandler;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

public class Main {
    public static void main(String[] args) {
        // 1. Parse named args
        String id = getArg(args, "--id");
        String port = getArg(args, "--port");

        if (id == null || port == null) {
            System.err.println("Usage: java -jar quorum.jar --id <id> --port <port> --peers <peer1:port1,peer2:port2>");
            System.exit(1);
        }

        String peersRaw = getArg(args, "--peers");
        List<String> peers = peersRaw != null
            ? Arrays.asList(peersRaw.split(","))
            : new ArrayList<>();

        // 2. Create objects in dependency order
        StateMachine stateMachine = new StateMachine();
        RaftNode node = new RaftNode(id, peers, stateMachine);
        MessageHandler messageHandler = new MessageHandler(node);
        RaftServer server = new RaftServer(port, messageHandler);

        // 3. Start server — this blocks forever inside server.start()
        System.out.println("[" + id + "] Starting on port " + port);
        server.start();
    }

    // Helper to find --flag value pairs in args array
    private static String getArg(String[] args, String flag) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(flag)) {
                return args[i + 1];
            }
        }
        return null;
    }
}
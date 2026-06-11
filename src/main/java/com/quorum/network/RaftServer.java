package com.quorum.network;

import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.quorum.client.ClientHandler;

public class RaftServer {
    private final String portString;
    private final MessageHandler messageHandler;
    private final ExecutorService threadPool;

    public RaftServer(String portString, MessageHandler messageHandler){
        this.portString = portString;
        this.messageHandler = messageHandler;
        this.threadPool = Executors.newFixedThreadPool(10);
    }
    public void start(){
        try (ServerSocket serverSocket = new ServerSocket(Integer.parseInt(portString))) {
            System.out.println("[Server] Listening on port " + portString);
            
            while (true) {
                // Blocks until someone connects
                Socket socket = serverSocket.accept();
                System.out.println("[Server] New connection from " 
                    + socket.getInetAddress());
                
                // Create handler for this connection
                ClientHandler clientHandler = 
                    new ClientHandler(socket, messageHandler);
                
                // Give it its own thread
                threadPool.submit(clientHandler);
            }
        } catch (IOException e) {
            System.err.println("[Server] Failed to start: " + e.getMessage());
        }
    }
}

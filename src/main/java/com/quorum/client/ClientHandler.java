package com.quorum.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import com.quorum.network.MessageHandler;

public class ClientHandler implements Runnable {
    private final Socket socket;
    private final MessageHandler messageHandler;

    public ClientHandler(Socket socket, MessageHandler messageHandler) {
        // assign fields
        this.socket = socket;
        this.messageHandler = messageHandler;
    }

    @Override
    public void run() {
        String clientAddress = socket.getInetAddress().getHostAddress();
        System.out.println("[ClientHandler] New connection from " + clientAddress);

        try {
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(socket.getInputStream()));
            PrintWriter writer = new PrintWriter(
                socket.getOutputStream(), true);

            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println("[ClientHandler] Received: " + line);
                String response = messageHandler.handle(line);
                System.out.println("[ClientHandler] Sending: " + response);
                writer.println(response);
            }

            System.out.println("[ClientHandler] Client disconnected: " + clientAddress);

        } catch (IOException e) {
            System.err.println("[ClientHandler] Error: " + e.getMessage());
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                System.err.println("[ClientHandler] Error closing socket: " + e.getMessage());
            }
        }
    }
}
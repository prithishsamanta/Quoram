package com.quorum.client;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class QuorumClient {
    
    private final String host;
    private final int port;
    
    public QuorumClient(String host, int port) {
        this.host = host;
        this.port = port;
    }
    
    public void start() {
        try {
            // 1. Create socket — this triggers accept() on the server
            Socket socket = new Socket(host, port);
            System.out.println("Connected to " + host + ":" + port);
            
            // 2. Open reader and writer on the socket
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(socket.getInputStream()));
            PrintWriter writer = new PrintWriter(
                socket.getOutputStream(), true);
            
            // 3. Read commands from terminal and send to server
            Scanner scanner = new Scanner(System.in);
            
            while (true) {
                // Read what user types in terminal
                System.out.print("> ");
                String input = scanner.nextLine();
                
                // Send to server through socket
                writer.println(input);
                
                // Read response from server
                String response = reader.readLine();
                System.out.println("Response: " + response);
                
                // Exit if user types quit
                if (input.equalsIgnoreCase("quit")) break;
            }
            
            socket.close();
            
        } catch (Exception e) {
            System.err.println("Connection failed: " + e.getMessage());
        }
    }
    
    public static void main(String[] args) {
        // Default to localhost:8001 if no args provided
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 8001;
        
        new QuorumClient(host, port).start();
    }
}
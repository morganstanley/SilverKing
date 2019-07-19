package com.ms.silverking.cloud.dht.client.example;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.NamespaceCreationException;
import com.ms.silverking.cloud.dht.client.OperationException;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;

public class SimpleShell {
    private final DHTSession    dhtSession;
    private SynchronousNamespacePerspective<String, String>    curNSP;
    
    public SimpleShell(String gridConfigName) throws IOException, ClientException {
        dhtSession = new DHTClient().openSession(SKGridConfiguration.parseFile(gridConfigName));
    }
    
    private void switchToNamespace(String ns) throws NamespaceCreationException {
        dhtSession.createNamespace(ns);
        curNSP = dhtSession.openSyncNamespacePerspective(ns, String.class, String.class);
    }
    
    public void commandLoop() throws IOException, OperationException, NamespaceCreationException {
        BufferedReader    reader;
        
        switchToNamespace("default");
        reader = new BufferedReader(new InputStreamReader(System.in)); 
        do {
            String[]    tokens;
            
            System.out.print(": ");
            tokens = reader.readLine().trim().split("\\s+");
            if (tokens[0].equalsIgnoreCase("namespace") && tokens.length == 2) {
                switchToNamespace(tokens[1]);
            } else if (tokens[0].equalsIgnoreCase("put") && tokens.length == 3) {
                curNSP.put(tokens[1], tokens[2]);
            } else if (tokens[0].equalsIgnoreCase("get") && tokens.length == 2) {
                System.out.println(curNSP.get(tokens[1]));
            } else if (tokens[0].equalsIgnoreCase("quit")) {
                break;
            } else {
                System.out.println("Invalid command");
            }
        } while (true);
    }
    
    public static void main(String[] args) {
        try {
            new SimpleShell(args[0]).commandLoop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

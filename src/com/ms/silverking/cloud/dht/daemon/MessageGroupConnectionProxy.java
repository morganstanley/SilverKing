package com.ms.silverking.cloud.dht.daemon;

import java.io.IOException;

import com.ms.silverking.cloud.dht.net.MessageGroupConnection;

/**
 * Provides the communication functionality required for operation processing while hiding whether
 * or not the actual communication is via an actual MessageGroupConnection or via an LWT worker.
 * 
 * This allows local communication to use the same methods as remote communication without incurring
 * the overhead of (local) network communication.
 */
interface MessageGroupConnectionProxy {
    void sendAsynchronous(Object data, long deadline) throws IOException;
    String getConnectionID();
    MessageGroupConnection getConnection();    
}

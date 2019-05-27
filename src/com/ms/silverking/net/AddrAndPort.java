package com.ms.silverking.net;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * Bundles a network address and port.
 */
public interface AddrAndPort {
    public static final String multipleDefDelimiter = ",";
    
    public InetSocketAddress toInetSocketAddress() throws UnknownHostException;
}

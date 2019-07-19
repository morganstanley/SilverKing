package com.ms.silverking.cloud.dht;

import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.numeric.NumConversion;

/**
 * Provides information identifying the creator of values. 
 * This typically consists of the IP address and process ID of the creator. 
 */
public interface ValueCreator {
    /**
     * Number of bytes in the byte[] representation of this ValueCreator.
     */
    public static final int BYTES = IPAddrUtil.IPV4_BYTES + NumConversion.BYTES_PER_INT;
    /**
     * Return the IP address of the creator.
     * @return the IP address of the creator.
     */
    public byte[] getIP();
    /**
     * Return the ID of the creator.
     * @return the ID of the creator.
     */
    public int getID();
    /**
     * Return the IP address and ID as bytes. 
     * @return the IP address and ID as bytes
     */
    public byte[] getBytes();
}

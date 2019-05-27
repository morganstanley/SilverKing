package com.ms.silverking.cloud.dht.common;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.numeric.NumConversion;

public class SimpleValueCreator implements ValueCreator {
    private final byte[]    bytes;
    
    private static final int    ipOffset = 0;
    private static final int    idOffset = IPAddrUtil.IPV4_BYTES;
    
    public SimpleValueCreator(byte[] bytes) {
        assert bytes != null;
        this.bytes = bytes;
    }
    
    public SimpleValueCreator(byte[] bytes, int offset) {
        this(Arrays.copyOfRange(bytes, offset, offset + ValueCreator.BYTES));
    }
    
    public SimpleValueCreator(ByteBuffer buf, int offset) {
        this(BufferUtil.arrayCopy(buf, offset, ValueCreator.BYTES));
    }
    
    public static SimpleValueCreator forLocalProcess() {
        try {
            byte[]  ip;
            int     pid;
            byte[]  bytes;
            
            pid = Integer.parseInt(ManagementFactory.getRuntimeMXBean().getName().split("\\@")[0]);
            ip = InetAddress.getLocalHost().getAddress();
            bytes = new byte[IPAddrUtil.IPV4_BYTES + NumConversion.BYTES_PER_INT];
            System.arraycopy(ip, 0, bytes, ipOffset, ip.length);
            NumConversion.intToBytes(pid, bytes, idOffset);
            return new SimpleValueCreator(bytes);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
    
    @Override
    public byte[] getIP() {
        byte[]  ip;
        
        ip = new byte[IPAddrUtil.IPV4_BYTES];
        System.arraycopy(bytes, ipOffset, ip, 0, ip.length);
        return ip;
    }

    @Override
    public int getID() {
        return NumConversion.bytesToInt(bytes, idOffset);
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }
    
    @Override
    public boolean equals(Object o) {
        ValueCreator    oVC;
        
        oVC = (ValueCreator)o;
        return areEqual(bytes, oVC.getBytes());                
    }
    
    public static boolean areEqual(byte[] vc1, byte[] vc2) {
        return java.util.Arrays.equals(vc1, vc2);                
    }
    
    @Override 
    public int hashCode() {
        return NumConversion.bytesToInt(bytes, ipOffset) ^ NumConversion.bytesToInt(bytes, idOffset);
    }
    
    @Override
    public String toString() {
        return IPAddrUtil.addrToString(getIP()) +":"+ getID();
    }
    
    public static void main(String[] args) {
        try {
            System.out.println(forLocalProcess().toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

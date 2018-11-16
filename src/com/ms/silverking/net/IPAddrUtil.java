package com.ms.silverking.net;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ms.silverking.numeric.NumConversion;

public class IPAddrUtil {
	public static final int	IPV4_BYTES = 4;
    public static final int IPV4_PORT_BYTES = 2;
    public static final int IPV4_IP_AND_PORT_BYTES = IPV4_BYTES + IPV4_PORT_BYTES;
    
    private static final int	MAX_IP_INT_VALUE = 255;
    
    private static Lock   classLock;
    
    private static final String ipProperty = "com.ms.silverking.net.IP";
    private static volatile byte[] localIP;
    private static volatile int    localIPInt;
    
    static {
        classLock = new ReentrantLock();
    }
    
    public static String localIPString() {
        return addrToString(localIP());
    }
    
    public static byte[] localIP() {
        if (localIPInt == 0) {
            initLocalIP();
        }
        return localIP;
    }
	
    public static int localIPInt() {
        if (localIPInt == 0) {
            initLocalIP();
        }
        return localIPInt;
    }
    
	private static void initLocalIP() {
        classLock.lock();
	    try {
	        if (localIPInt == 0) {
	            String ipDef;
	            
	            ipDef = System.getProperty(ipProperty);
	            if (ipDef != null) {
	                localIP = IPAddrUtil.stringToAddr(ipDef);
	            } else {
	                try {
	                    localIP = InetAddress.getLocalHost().getAddress();
	                } catch (UnknownHostException uhe) {
	                    throw new RuntimeException(uhe);
	                }
	            }
	            localIPInt = addrToInt(localIP);
	        }
	    } finally {
	        classLock.unlock();
	    }        
    }

    public static int addrToInt(byte[] addr) {
        return addrToInt(addr, 0);
    }
    
    public static int addrToInt(byte[] addr, int offset) {
	    return NumConversion.bytesToInt(addr, offset);
	}
	
    public static byte[] intToAddr(int intAddr) {
        byte[]  addr;
        
        addr = new byte[IPV4_BYTES];
        NumConversion.intToBytes(intAddr, addr, 0);
        return addr;
    }
    
    public static void intToAddr(int intAddr, byte[] addr) {
        NumConversion.intToBytes(intAddr, addr, 0);
    }
    
    public static void intToAddr(int intAddr, byte[] addr, int offset) {
        NumConversion.intToBytes(intAddr, addr, offset);
    }
    
    public static byte[] createIPAndPort(byte[] ip, int port) {
        byte[]  ipAndPort;
        
        ipAndPort = new byte[IPV4_IP_AND_PORT_BYTES];
        System.arraycopy(ip, 0, ipAndPort, 0, IPV4_BYTES);
        NumConversion.unsignedShortToBytes(port, ipAndPort, IPV4_BYTES);
        return ipAndPort;
    }
    
	private static final void verifyAddrLength(byte[] addr) {
	    verifyLength(addr, IPV4_BYTES);
	}
	
    private static final void verifyLength(byte[] addr, int expectedLength) {
        if (addr.length != expectedLength) {
            throw new RuntimeException("bad length");
        }
    }
    
	public static int compare(byte[] addr1, byte[] addr2) {
		verifyAddrLength(addr1);
		verifyAddrLength(addr2);
		for (int i = 0; i < IPV4_BYTES; i++) {
			if ( NumConversion.unsignedByteToInt(addr1[i]) 
					< NumConversion.unsignedByteToInt(addr2[i])) {
				return -1;
			} else if ( NumConversion.unsignedByteToInt(addr1[i]) 
					> NumConversion.unsignedByteToInt(addr2[i])) {
				return 1;
			}
		}
		return 0;
	}
	
	public static byte[] stringToAddr(String s) {
		byte[] addr;
		
		addr = new byte[IPV4_BYTES];
		stringToAddr(s, addr);
		return addr;
	}
	
	public static void stringToAddr(String s, byte[] addr) {
		String[]	tokens;
		
		verifyAddrLength(addr);
		tokens = s.split("\\.");
		for (int i = 0; i < tokens.length; i++) {
			int	val;
			
			val = Integer.parseInt(tokens[i]);
			if (val > MAX_IP_INT_VALUE) {
				throw new RuntimeException("Invalid IP: "+ s);
			}
			addr[i] = (byte)val;
		}
	}
	
	/**
	 * Checks to see if the string is a valid IP. Does not check to see if the IP actually exists.
	 * @return
	 */
	public static boolean isValidIP(String s) {
		try {
			stringToAddr(s);
			return true;
		} catch (Exception e) {
			return false;
		}
	}
	
    public static String addrToString(int _addr) {
        byte[] addr;
        
        addr = new byte[IPV4_BYTES];
        intToAddr(_addr, addr);
        return addrToString(addr);
    }
    
	public static String addrToString(byte[] addr, int offset) {
		StringBuilder	sb;
		
		sb = new StringBuilder();
		addrToString(sb, addr, offset);
		return sb.toString();
	}
	
	public static String addrToString(byte[] addr) {
		StringBuilder	sb;
		
		verifyAddrLength(addr);
		sb = new StringBuilder();
		addrToString(sb, addr);
		return sb.toString();
	}
	
    private static void addrToString(StringBuilder sb, byte[] addr) {
        addrToString(sb, addr, 0);
    }
    
    private static void addrToString(StringBuilder sb, byte[] addr, int offset) {
        addrToString(sb, addr[offset + 0], addr[offset + 1], addr[offset + 2], addr[offset + 3]);
    }
    
    private static void addrToString(StringBuilder sb, byte a0, byte a1, byte a2, byte a3) {
        sb.append(NumConversion.unsignedByteToInt(a0));
        sb.append('.');
        sb.append(NumConversion.unsignedByteToInt(a1));
        sb.append('.');
        sb.append(NumConversion.unsignedByteToInt(a2));
        sb.append('.');
        sb.append(NumConversion.unsignedByteToInt(a3));
    }
    
    public static String addrAndPortToString(byte[] addr) {
        StringBuilder   sb;
        
        verifyLength(addr, IPV4_IP_AND_PORT_BYTES);
        sb = new StringBuilder();
        addrToString(sb, addr);
        sb.append(':');
        sb.append(NumConversion.bytesToUnsignedShort(addr, IPV4_BYTES));
        return sb.toString();
    }
    
    public static byte[] serverNameToAddr(String serverName) throws UnknownHostException {
        return InetAddress.getByName(serverName).getAddress();
    }
}

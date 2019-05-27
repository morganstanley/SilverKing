package com.ms.silverking.net;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;

public class InetAddressUtil {
	/**
	 * Determine if a given InetAddress corresponds to any local IP address.
	 * @param addr  the InetAddress to check
	 * @return true if addr corresponds to any local IPAddress
	 */
	public static boolean isLocalHostIP(InetAddress addr) {
	    if (addr.isAnyLocalAddress() || addr.isLoopbackAddress()) {
	        return true;
	    }
	    try {
	        return NetworkInterface.getByInetAddress(addr) != null;
	    } catch (SocketException e) {
	        return false;
	    }
	}
}

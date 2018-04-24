package com.ms.silverking.cloud.dht.meta;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;

public class Util {

	static final int portMin = 4_000;
	static final int portMax = 10_000;
	
	static int getFreePort() {
		return getFreePort(portMin, portMax);
	}
	
	static int getFreePort(int min, int max) {
		int numAttempts = 5;
		for (int i = 0; i < numAttempts; i++) {
			int port = getRandomPort(min, max);
			if ( isFree(port) ) {
				return port;
			}
		}
		
		return StaticDHTCreatorOptions.defaultPort;
	}
	
	static int getRandomPort(int min, int max) {
		Random r = new Random();
		int port = r.nextInt(max-min) + min;
		return port;
	}
	
	static boolean isFree(int port) {
		try {
	        new ServerSocket(port).close();
	        return true;
	    } catch (IOException e) {
	        return false;
	    }
	}
}

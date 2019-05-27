package com.ms.silverking.cloud.dht.client;

import static com.ms.silverking.cloud.dht.client.OpTimeoutController.min_maxAttempts;

class Util {
    static void checkAttempts(int maxAttempts) {
    	if (maxAttempts < min_maxAttempts) {
    		throw new RuntimeException("maxAttempts < min_maxAttempts; "+ maxAttempts +" < "+ min_maxAttempts);
    	}
    }
}

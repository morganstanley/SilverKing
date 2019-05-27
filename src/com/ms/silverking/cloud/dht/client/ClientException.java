package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.cloud.dht.client.gen.NonVirtual;

/**
 * Base DHT client exception class.
 *
 */
@NonVirtual
public class ClientException extends Exception {
	public ClientException() {
		super();
	}

	public ClientException(String arg0) {
		super(arg0);
	}

	public ClientException(Throwable arg0) {
		super(arg0);
	}

	public ClientException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}
}

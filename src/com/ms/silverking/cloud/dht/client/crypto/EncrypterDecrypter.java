package com.ms.silverking.cloud.dht.client.crypto;


public interface EncrypterDecrypter extends Encrypter, Decrypter {
	public static final String keyFilePropertyName = "com.ms.silverking.compression.KeyFile";

	public String getName();
}

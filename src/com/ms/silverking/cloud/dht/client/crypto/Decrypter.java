package com.ms.silverking.cloud.dht.client.crypto;


public interface Decrypter {
	public byte[] decrypt(byte[] cipherText, int offset, int length);
}

package com.ms.silverking.cloud.dht.client.crypto;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import com.ms.silverking.io.FileUtil;

/**
 * Trivial EncrypterDecrypter. This implementation provides basic obfuscation only.
 */
public class XOREncrypterDecrypter implements EncrypterDecrypter {
	private final byte[] key;
	
	public static final String name = "xor";
	
	public XOREncrypterDecrypter(byte[] key) {
		this.key = key;
	}
	
	public XOREncrypterDecrypter(File file) throws IOException {
		this(FileUtil.readFileAsBytes(file));
	}	
	
	public XOREncrypterDecrypter() throws IOException {
		this(Util.getBytesFromKeyFile());
	}
	
	@Override
	public String getName() {
		return name;
	}
	
	@Override
	public byte[] encrypt(byte[] plainText) {
		byte[]	cipherText;
		
		cipherText = new byte[plainText.length];
		for (int i = 0; i < plainText.length; i++) {
			cipherText[i] = (byte)(plainText[i] ^ key[i % key.length]);
		}
		//System.out.printf("plainText:  %s\n", StringUtil.byteArrayToHexString(plainText));
		//System.out.printf("cipherText: %s\n", StringUtil.byteArrayToHexString(cipherText));
		return cipherText;
	}

	@Override
	public byte[] decrypt(byte[] cipherText, int offset, int length) {
		byte[]	plainText;
		
		plainText = new byte[length];
		for (int i = 0; i < length; i++) {
			plainText[i] = (byte)(cipherText[offset + i] ^ key[i % key.length]);
		}
		//System.out.printf("cipherText: %s\n", StringUtil.byteArrayToHexString(cipherText, offset, length));
		//System.out.printf("plainText:  %s\n", StringUtil.byteArrayToHexString(plainText));
		//System.out.printf("plainTextS: %s\n", new String(plainText));
		return plainText;
	}
	
	@Override
	public int hashCode() {
		return Arrays.hashCode(key);
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		
		if (this.getClass() != o.getClass()) {
			return false;
		}
		
		XOREncrypterDecrypter other = (XOREncrypterDecrypter)o;
		return Arrays.equals(key, other.key);
	}
}

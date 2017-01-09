package com.ms.silverking.cloud.dht.client.crypto;

import java.io.File;
import java.io.IOException;

import com.ms.silverking.io.FileUtil;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.UndefinedAction;

/**
 * Trivial EncrypterDecrypter. This implementation provides basic obfuscation only.
 */
public class XOREncrypterDecrypter implements EncrypterDecrypter {
	private final byte[]	key;
	
	public static final String	name = "xor";
	
	public XOREncrypterDecrypter(byte[] key) {
			this.key = key;
	}
	
	public XOREncrypterDecrypter(File file) throws IOException {
		this(FileUtil.readFileAsBytes(file));
	}	
	
	public XOREncrypterDecrypter() throws IOException {
		this(FileUtil.readFileAsBytes(new File(PropertiesHelper.systemHelper.getString(EncrypterDecrypter.keyFilePropertyName, UndefinedAction.ExceptionOnUndefined))));
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
}

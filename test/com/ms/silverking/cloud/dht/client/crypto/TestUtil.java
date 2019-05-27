package com.ms.silverking.cloud.dht.client.crypto;

import static com.ms.silverking.testing.Util.byte_maxVal;
import static com.ms.silverking.testing.Util.byte_minVal;
import static com.ms.silverking.testing.Util.createToString;
import static com.ms.silverking.testing.Util.getTestMessage;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestUtil {
	
	public static final byte[] negOne     = {-1};
	public static final byte[] negOneCopy = {-1};	// important to make copy rather than re-use, negOne byte array, for testing equals and hashCode
	public static final byte[] empty      = {};
	public static final byte[] posOne     = {1};
	public static final byte[] byte_minToMax     = {byte_minVal, -1, 0, 1, byte_maxVal};
	public static final byte[] byte_minToMaxCopy = {byte_minVal, -1, 0, 1, byte_maxVal};	// important to make copy rather than re-use, byte_minToMax byte array, for testing equals and hashCode

	public static final byte[][] testCases = {negOne, empty, posOne, byte_minToMax};

	public static final AESEncrypterDecrypter m2mAESCopy = createAES(byte_minToMaxCopy);
	public static final XOREncrypterDecrypter m2mXORCopy = createXOR(byte_minToMaxCopy);
	
	public static AESEncrypterDecrypter createAES(byte[] bytes) {
		return new AESEncrypterDecrypter(bytes);
	}

	public static XOREncrypterDecrypter createXOR(byte[] bytes) {
		return new XOREncrypterDecrypter(bytes);
	}
	
	public static void checkName(String expected, String actual) {
		assertEquals(expected, actual);
	}
	
	public static void checkEncryptDecrypt(byte[] expected, EncrypterDecrypter ec) {
		byte[] encrypted = ec.encrypt(expected);
		byte[] decrypted = ec.decrypt(encrypted, 0, encrypted.length);
		assertArrayEquals( getTestMessage("encryptDecrypt", ec, createToString(expected), createToString(decrypted)), expected, decrypted);
	}
}

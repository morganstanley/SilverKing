package com.ms.silverking.cloud.dht.client.crypto;

import static com.ms.silverking.cloud.dht.client.crypto.TestUtil.*;
import static com.ms.silverking.cloud.dht.client.crypto.TestUtil.empty;
import static com.ms.silverking.cloud.dht.client.crypto.TestUtil.m2mXORCopy;
import static com.ms.silverking.cloud.dht.client.crypto.TestUtil.negOne;
import static com.ms.silverking.cloud.dht.client.crypto.TestUtil.negOneCopy;
import static com.ms.silverking.cloud.dht.client.crypto.TestUtil.posOne;
import static com.ms.silverking.cloud.dht.client.crypto.TestUtil.testCases;
import static com.ms.silverking.testing.AssertFunction.*;

import java.io.IOException;

import org.junit.Test;

import com.ms.silverking.util.PropertyException;

public class XOREncrypterDecrypterTest {
	
	private static final XOREncrypterDecrypter negOneXOR     = createXOR(negOne);
	private static final XOREncrypterDecrypter negOneXORCopy = createXOR(negOneCopy);
	private static final XOREncrypterDecrypter emptyXOR      = createXOR(empty);
	private static final XOREncrypterDecrypter posOneXOR     = createXOR(posOne);
	private static final XOREncrypterDecrypter m2mXOR        = createXOR(byte_minToMax);
	
	private static final XOREncrypterDecrypter[][] testCasesEquals = {
		{negOneXOR, negOneXORCopy},
		{emptyXOR,  emptyXOR},
		{posOneXOR, posOneXOR},
		{m2mXOR,    m2mXORCopy},
	};
	private static final EncrypterDecrypter[][] testCasesNotEquals = {
		{negOneXOR, emptyXOR},
		{emptyXOR,  posOneXOR},
		{posOneXOR, m2mXOR},
		{m2mXOR,    negOneXOR},
		{m2mXOR,    m2mAESCopy},	// xor vs aes
	};

	@Test(expected=PropertyException.class)
	public void testConstructors_Exception() throws IOException {
		new XOREncrypterDecrypter();
	}
	
	@Test
	public void testConstructors() {
		for (byte[] testCase : testCases)
			createXOR(testCase);
	}

	@Test
	public void testGetName() {
		checkName("xor", XOREncrypterDecrypter.name);
	}

	@Test
	public void testEncryptDecrypt() {
		for (byte[] testCase : testCases) {
			XOREncrypterDecrypter ec = createXOR(testCase);
			checkEncryptDecrypt(testCase, ec);
		}
	}
	
	@Test
	public void testHashCode() {
		test_HashCodeEquals(   testCasesEquals);
		test_HashCodeNotEquals(testCasesNotEquals);
	}

	@Test
	public void testEquals() {
		test_Equals(   testCasesEquals);
		test_NotEquals(testCasesNotEquals);
	}
}

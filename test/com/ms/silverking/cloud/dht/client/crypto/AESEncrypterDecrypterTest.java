package com.ms.silverking.cloud.dht.client.crypto;

import static com.ms.silverking.cloud.dht.client.crypto.TestUtil.*;
import static com.ms.silverking.cloud.dht.client.crypto.TestUtil.empty;
import static com.ms.silverking.cloud.dht.client.crypto.TestUtil.m2mXORCopy;
import static com.ms.silverking.cloud.dht.client.crypto.TestUtil.negOne;
import static com.ms.silverking.cloud.dht.client.crypto.TestUtil.negOneCopy;
import static com.ms.silverking.cloud.dht.client.crypto.TestUtil.posOne;
import static com.ms.silverking.cloud.dht.client.crypto.TestUtil.testCases;
import static com.ms.silverking.testing.AssertFunction.test_EqualsOrNotEquals;
import static com.ms.silverking.testing.AssertFunction.test_HashCode;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import com.ms.silverking.util.PropertyException;

public class AESEncrypterDecrypterTest {

	private static final AESEncrypterDecrypter negOneAES     = createAES(negOne);
	private static final AESEncrypterDecrypter negOneAESCopy = createAES(negOneCopy);
	private static final AESEncrypterDecrypter emptyAES      = createAES(empty);
	private static final AESEncrypterDecrypter posOneAES     = createAES(posOne);
	private static final AESEncrypterDecrypter m2mAES        = createAES(byte_minToMax);
	
	private static final Object[][] testCasesEqualsNotEquals = {
		// equals
//		{negOneAES, negOneAESCopy, true},
		{emptyAES,  emptyAES,      true},
		{posOneAES, posOneAES,     true},
//		{m2mAES,    m2mAESCopy,    true},
		// not equals
		{negOneAES, emptyAES,   false},
		{emptyAES,  posOneAES,  false},
		{posOneAES, m2mAES,     false},
		{m2mAES,    negOneAES,  false},
		{m2mAES,    m2mXORCopy, false},	// aes vs xor
	};
	
	@Test(expected=PropertyException.class)
	public void testConstructors_Exception() throws IOException {
		new AESEncrypterDecrypter();
	}
	
	@Test
	public void testConstructors() {
		for (byte[] testCase : testCases)
			createAES(testCase);
	}

	@Test
	public void testGetName() {
		assertEquals("AES", AESEncrypterDecrypter.name);
	}

	@Test
	public void testEncryptDecrypt() {
		for (byte[] testCase : testCases) {
			AESEncrypterDecrypter ec = createAES(testCase);
			checkEncryptDecrypt(testCase, ec);
		}
	}
	
	@Test
	public void testHashCode() throws IOException {
		test_HashCode(testCasesEqualsNotEquals);
	}

	@Test
	public void testEquals() {
		test_EqualsOrNotEquals(testCasesEqualsNotEquals);
	}
}

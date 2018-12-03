package com.ms.silverking.cloud.dht.client.crypto;

import static com.ms.silverking.cloud.dht.client.crypto.TestUtil.*;
import static com.ms.silverking.testing.AssertFunction.*;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import com.ms.silverking.testing.Util;
import com.ms.silverking.util.PropertyException;

public class AESEncrypterDecrypterTest {

	private static final AESEncrypterDecrypter negOneAES     = createAES(negOne);
	private static final AESEncrypterDecrypter negOneAESCopy = createAES(negOneCopy);
	private static final AESEncrypterDecrypter emptyAES      = createAES(empty);
	private static final AESEncrypterDecrypter posOneAES     = createAES(posOne);
	private static final AESEncrypterDecrypter m2mAES        = createAES(byte_minToMax);
	
	private static final AESEncrypterDecrypter[][] testCasesEquals = {
//FIXME:bph: why are these commented out?		{negOneAES, negOneAESCopy},
		{emptyAES,  emptyAES},
		{posOneAES, posOneAES},
//FIXME:bph: why are these commented out?		{m2mAES,    m2mAESCopy},
	};
	private static final EncrypterDecrypter[][] testCasesNotEquals = {
		{negOneAES, emptyAES},
		{emptyAES,  posOneAES},
		{posOneAES, m2mAES},
		{m2mAES,    negOneAES},
		{m2mAES,    m2mXORCopy},	// aes vs xor
	};
	
	@Test(expected=PropertyException.class)
	public void testConstructors_Exception() throws IOException {
		new AESEncrypterDecrypter();
	}

	// already tested in static variables above, and ctor 'secureRandom.generateSeed(16)' takes a lot of time, so commenting out is to speed up overall test runtime
//	@Test
//	public void testConstructors() {
//		for (byte[] testCase : testCases)
//			createAES(testCase);
//	}

	@Test
	public void testGetName() {
		assertEquals("AES", AESEncrypterDecrypter.name);
	}

	@Test
	public void testEncryptDecrypt() {
//		for (byte[] testCase : testCases) {
//			AESEncrypterDecrypter ec = createAES(testCase);
//			checkEncryptDecrypt(testCase, ec);
//		}
		// use already created AES obj's to speed up test runtime
		Object[][] testCases = {
			{negOne,        negOneAES},
			{empty,          emptyAES},
			{posOne,        posOneAES},
			{byte_minToMax,    m2mAES},
		};
		for (Object[] testCase : testCases) { 
			byte[] expected          =                (byte[])testCase[0];
			AESEncrypterDecrypter ec = (AESEncrypterDecrypter)testCase[1];
			checkEncryptDecrypt(expected, ec);
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
	
	public static void main(String[] args) throws IOException {
		Util.runTests(AESEncrypterDecrypterTest.class);
	}
}

package com.ms.silverking.cloud.dht.client.crypto;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.KeySpec;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;

import com.ms.silverking.io.FileUtil;
import com.ms.silverking.io.StreamUtil;

/**
 * AES EncrypterDecrypter
 */
public class AESEncrypterDecrypter implements EncrypterDecrypter {
	private final SecretKey secretKey;
	private final IvParameterSpec iv;
	
	public static final String	name = "AES";

	private static final int saltLength = 8;
	
	private static SecretKey generateKey() throws NoSuchAlgorithmException {
		KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
		keyGenerator.init(128);
		SecretKey key = keyGenerator.generateKey();
		return key;
	}
	
	public AESEncrypterDecrypter(byte[] key) {
		//for (Object obj : java.security.Security.getAlgorithms("Cipher")) {
		//	System.out.println(obj);
		//}

		try {
//			byte[] salt;
//			char[] password;
//			String keyFile;
			SecureRandom secureRandom;
			secureRandom = new SecureRandom();
			iv = new IvParameterSpec(secureRandom.generateSeed(16));
//			salt = new byte[saltLength];
//			secureRandom.nextBytes(salt);
//			SecretKeyFactory factory = SecretKeyFactory
//					.getInstance("PBKDF2WithHmacSHA256");
//			password = new String(StreamUtil.readToBytes(new ByteArrayInputStream(key), true)).toCharArray();
//			KeySpec spec = new PBEKeySpec(password, salt, 65536, 128);
			secretKey = generateKey();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public AESEncrypterDecrypter(File file) throws IOException {
		this(FileUtil.readFileAsBytes(file));
	}	
	
	public AESEncrypterDecrypter() throws IOException {
		this(Util.getBytesFromKeyFile());
	}
	
	private Cipher getCipher(int mode) {
		if (secretKey == null) {
			throw new RuntimeException("No key defined");
		} else {
			try {
				Cipher	cipher;
				
				cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
				cipher.init(mode, secretKey, iv);
				return cipher;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	@Override
	public String getName() {
		return name;
	}

	@Override
	public byte[] encrypt(byte[] plainText) {
		byte[] cipherText;
		Cipher cipher;
		
		cipher = getCipher(Cipher.ENCRYPT_MODE);
		try {
			//cipher.update(NumConversion.intToBytes(plainText.length));
			cipherText = cipher.doFinal(plainText);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		//System.out.printf("plainText:  %s\n",
		//StringUtil.byteArrayToHexString(plainText));
		//System.out.printf("cipherText: %s\n",
		//StringUtil.byteArrayToHexString(cipherText));
		return cipherText;
	}

	@Override
	public byte[] decrypt(byte[] cipherTextWithLength, int offset, int length) {
		byte[] plainText;
		Cipher cipher;
		
//		System.out.printf("%d %d\n", offset, length);
//		System.out.printf("cipherText: %s\n",
//		StringUtil.byteArrayToHexString(cipherTextWithLength, offset, length));
		cipher = getCipher(Cipher.DECRYPT_MODE);
		try {
			plainText = cipher.doFinal(cipherTextWithLength,
										offset, length);
									   //offset + NumConversion.BYTES_PER_INT, 
									   //length - NumConversion.BYTES_PER_INT);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
//		System.out.printf("plainText:  %s\n",
//		StringUtil.byteArrayToHexString(plainText));
//		System.out.printf("plainTextS: %s\n", new String(plainText));
		return plainText;
	}
	
	@Override
	public int hashCode() {
		return secretKey.hashCode() ^ iv.hashCode();
	}
	
    @Override
    public boolean equals(Object o) {
    	if (this == o) {
    		return true;
    	}
    	
    	if (this.getClass() != o.getClass()) {
    		return false;
    	}
    	
    	AESEncrypterDecrypter other = (AESEncrypterDecrypter)o;
    	return secretKey.equals(other.secretKey) && iv.equals(other.iv);
    }
	
	@Override
	public String toString() {
		return "[Name: " + name + ", Salt length: " + saltLength + ", secretKey: " + " , iv: " + "]";
	}
}

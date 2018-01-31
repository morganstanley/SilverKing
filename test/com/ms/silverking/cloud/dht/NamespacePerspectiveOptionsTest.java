package com.ms.silverking.cloud.dht;

import static com.ms.silverking.cloud.dht.NamespacePerspectiveOptions.*;
import static com.ms.silverking.cloud.dht.TestUtil.*;
import static com.ms.silverking.cloud.dht.common.DHTConstants.*;
import static com.ms.silverking.testing.AssertFunction.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.ms.silverking.cloud.dht.client.KeyDigestType;
import com.ms.silverking.cloud.dht.client.VersionProvider;
import com.ms.silverking.cloud.dht.client.crypto.EncrypterDecrypter;
import com.ms.silverking.testing.Util.ExceptionChecker;

public class NamespacePerspectiveOptionsTest {

	private static final NamespacePerspectiveOptions<byte[], byte[]> defaultNspOptions                     = NamespacePerspectiveOptions.templateOptions;
	private static final NamespacePerspectiveOptions<byte[], byte[]> defaultNspOptionsCopy                 = TestUtil.getCopy();
	private static final NamespacePerspectiveOptions<IllegalArgumentException, Enum> defaultNspOptionsDiff = TestUtil.getDiff();
	
	private Class<?> getKeyClass(NamespacePerspectiveOptions<?, ?> nspOptions) {
		return nspOptions.getKeyClass();
	}
	
	private Class<?> getValueClass(NamespacePerspectiveOptions<?, ?> nspOptions) {
		return nspOptions.getValueClass();
	}
	
	private KeyDigestType getKeyDigestType(NamespacePerspectiveOptions<?, ?> nspOptions) {
		return nspOptions.getKeyDigestType();
	}
	
	private PutOptions getDefaultPutOptions(NamespacePerspectiveOptions<?, ?> nspOptions) {
		return nspOptions.getDefaultPutOptions();
	}
	
	private InvalidationOptions getDefaultInvalidationOptions(NamespacePerspectiveOptions<?, ?> nspOptions) {
		return nspOptions.getDefaultInvalidationOptions();
	}
	
	private GetOptions getDefaultGetOptions(NamespacePerspectiveOptions<?, ?> nspOptions) {
		return nspOptions.getDefaultGetOptions();
	}
	
	private WaitOptions getDefaultWaitOptions(NamespacePerspectiveOptions<?, ?> nspOptions) {
		return nspOptions.getDefaultWaitOptions();
	}
	
	private VersionProvider getDefaultVersionProvider(NamespacePerspectiveOptions<?, ?> nspOptions) {
		return nspOptions.getDefaultVersionProvider();
	}
	
	private EncrypterDecrypter getEncrypterDecrypter(NamespacePerspectiveOptions<?, ?> nspOptions) {
		return nspOptions.getEncrypterDecrypter();
	}
	
	private NamespacePerspectiveOptions<?, ?> setKeyClass(Class<?> kc) {
		return defaultNspOptions.keyClass(kc);
	}
	
	private NamespacePerspectiveOptions<?, ?> setValueClass(Class<?> vc) {
		return defaultNspOptions.valueClass(vc);
	}
	
	private NamespacePerspectiveOptions<?, ?> setKeyDigestType(KeyDigestType kdt) {
		return defaultNspOptions.keyDigestType(kdt);
	}
	
	private NamespacePerspectiveOptions<?, ?> setDefaultPutOptions(PutOptions po) {
		return defaultNspOptions.defaultPutOptions(po);
	}
	
	private NamespacePerspectiveOptions<?, ?> setDefaultInvalidationOptions(InvalidationOptions io) {
		return defaultNspOptions.defaultInvalidationOptions(io);
	}
	
	private NamespacePerspectiveOptions<?, ?> setDefaultGetOptions(GetOptions go) {
		return defaultNspOptions.defaultGetOptions(go);
	}
	
	private NamespacePerspectiveOptions<?, ?> setDefaultWaitOptions(WaitOptions wo) {
		return defaultNspOptions.defaultWaitOptions(wo);
	}
	
	private NamespacePerspectiveOptions<?, ?> setDefaultVersionProvider(VersionProvider vp) {
		return defaultNspOptions.defaultVersionProvider(vp);
	}
	
	private NamespacePerspectiveOptions<?, ?> setEncrypterDecrypter(EncrypterDecrypter ed) {
		return defaultNspOptions.encrypterDecrypter(ed);
	}
	
	@Test
	public void testGetters() {
		Object[][] testCases = {
			{byte[].class,                getKeyClass(defaultNspOptions)},
			{byte[].class,                getValueClass(defaultNspOptions)},
			{standardKeyDigestType,       getKeyDigestType(defaultNspOptions)},
			{standardPutOptions,          getDefaultPutOptions(defaultNspOptions)},
			{standardInvalidationOptions, getDefaultInvalidationOptions(defaultNspOptions)},
			{standardGetOptions,          getDefaultGetOptions(defaultNspOptions)},
			{standardWaitOptions,         getDefaultWaitOptions(defaultNspOptions)},
			{standardVersionProvider,     getDefaultVersionProvider(defaultNspOptions)},
			{defaultEncrypterDecrypter,   getEncrypterDecrypter(defaultNspOptions)},
		};
		
		test_Getters(testCases);
	}
	
	// this takes care of testing ctors as well
	@Test
	public void testSetters_Exceptions() {
		Object[][] testCases = {
			{"keyClass = null",                         new ExceptionChecker() { @Override public void check() { setKeyClass(null);                                 } },     NullPointerException.class},
			{"valueClass = null",                       new ExceptionChecker() { @Override public void check() { setValueClass(null);                               } },     NullPointerException.class},
			{"keyDigestType= null",                     new ExceptionChecker() { @Override public void check() { setKeyDigestType(null);                            } },     NullPointerException.class},
			{"defaultPutOptions = null",                new ExceptionChecker() { @Override public void check() { setDefaultPutOptions(null);                        } },     NullPointerException.class},
			{"defaultPutOptions = invalidationOptions", new ExceptionChecker() { @Override public void check() { setDefaultPutOptions(standardInvalidationOptions); } }, IllegalArgumentException.class},
			{"defaultInvalidationOptions = null",       new ExceptionChecker() { @Override public void check() { setDefaultInvalidationOptions(null);               } },     NullPointerException.class},
			{"defaultGetOptions = null",                new ExceptionChecker() { @Override public void check() { setDefaultGetOptions(null);                        } },     NullPointerException.class},
			{"defaultWaitOptions = null",               new ExceptionChecker() { @Override public void check() { setDefaultWaitOptions(null);                       } },     NullPointerException.class},
			{"defaultVersionProvider = null",           new ExceptionChecker() { @Override public void check() { setDefaultVersionProvider(null);                   } },     NullPointerException.class},
		};

		test_SetterExceptions(testCases);
	}
	
	@Test
	public void testSetters() {
		for (KeyDigestType type : KeyDigestType.values())
			assertEquals(type, getKeyDigestType( setKeyDigestType(type) ));	
		
		Object[][] testCases = {
			{kcDiff,  getKeyClass( setKeyClass(kcDiff) )},
			{vcDiff,  getValueClass( setValueClass(vcDiff) )},
			{poDiff,  getDefaultPutOptions( setDefaultPutOptions(poDiff) )},
			{ioDiff,  getDefaultInvalidationOptions( setDefaultInvalidationOptions(ioDiff) )},
			{goDiff,  getDefaultGetOptions( setDefaultGetOptions(goDiff) )},
			{woDiff,  getDefaultWaitOptions(setDefaultWaitOptions(woDiff) )},
			{vpDiff,  getDefaultVersionProvider( setDefaultVersionProvider(vpDiff) )},
			{edDiff,  getEncrypterDecrypter( setEncrypterDecrypter(edDiff) )},
		};
		
		test_Setters(testCases);
	}
	
	@Test
	public void testHashCode() {
		checkHashCodeEquals(   defaultNspOptions, defaultNspOptions);
		checkHashCodeEquals(   defaultNspOptions, defaultNspOptionsCopy);
		checkHashCodeNotEquals(defaultNspOptions, defaultNspOptionsDiff);
	}
	
	@Test
	public void testEqualsObject() {
		Object[][] testCases = {
			{defaultNspOptions,     defaultNspOptions,                     defaultNspOptionsDiff},
			{defaultNspOptionsDiff, defaultNspOptionsDiff,                 defaultNspOptions},
			{defaultNspOptionsCopy, defaultNspOptions,                     defaultNspOptionsDiff},
			{defaultNspOptions,     setKeyClass(kcCopy),                   setKeyClass(kcDiff)},
			{defaultNspOptions,     setValueClass(vcCopy),                 setValueClass(vcDiff)},
			{defaultNspOptions,     setKeyDigestType(kdtCopy),             setKeyDigestType(kdtDiff)},
			{defaultNspOptions,     setDefaultPutOptions(poCopy),          setDefaultPutOptions(poDiff)},
			{defaultNspOptions,     setDefaultInvalidationOptions(ioCopy), setDefaultInvalidationOptions(ioDiff)},
			{defaultNspOptions,     setDefaultGetOptions(goCopy),          setDefaultGetOptions(goDiff)},
			{defaultNspOptions,     setDefaultWaitOptions(woCopy),         setDefaultWaitOptions(woDiff)},
			{defaultNspOptions,     setDefaultVersionProvider(vpCopy),     setDefaultVersionProvider(vpDiff)},
			{defaultNspOptions,     setEncrypterDecrypter(edCopy),         setEncrypterDecrypter(edDiff)},
		};
		
		test_FirstEqualsSecond_SecondNotEqualsThird(testCases);
		test_NotEquals(new Object[][]{
			{defaultNspOptions, NamespaceOptions.templateOptions},
		});
	}

	@Test
	public void testToStringAndParse() {
		NamespacePerspectiveOptions<?, ?>[] testCases = {
			defaultNspOptions,
			defaultNspOptionsCopy,
			defaultNspOptionsDiff,
		};
		
		for (NamespacePerspectiveOptions<?, ?> testCase : testCases)
			checkStringAndParse(testCase);
	}
	
	private void checkStringAndParse(NamespacePerspectiveOptions<?, ?> nspOptions) {
//		assertEquals(nspOptions, nspOptions.parse( nspOptions.toString() ));
	}
}

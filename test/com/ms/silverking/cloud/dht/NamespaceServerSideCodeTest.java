package com.ms.silverking.cloud.dht;

import static com.ms.silverking.testing.AssertFunction.checkHashCodeEquals;
import static com.ms.silverking.testing.AssertFunction.checkHashCodeNotEquals;
import static com.ms.silverking.testing.AssertFunction.test_FirstEqualsSecond_FirstNotEqualsThird;
import static com.ms.silverking.testing.AssertFunction.test_Getters;
import static com.ms.silverking.testing.AssertFunction.test_NotEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class NamespaceServerSideCodeTest {

	private static final String urlCopy = "";
	private static final String urlDiff = "a";
	
	private static final String putTriggerCopy = "";
	private static final String putTriggerDiff = "b";
	
	private static final String retrieveTriggerCopy = "";
	private static final String retrieveTriggerDiff = "c";
	
	private static final NamespaceServerSideCode defaultCode      =     NamespaceServerSideCode.template;
	private static final NamespaceServerSideCode defaultCodeCopy  = new NamespaceServerSideCode(urlCopy, putTriggerCopy, retrieveTriggerCopy);
	private static final NamespaceServerSideCode defaultCodeDiff  = new NamespaceServerSideCode(urlDiff, putTriggerDiff, retrieveTriggerDiff);
	private static final NamespaceServerSideCode defaultCodeNull1 = new NamespaceServerSideCode(null, null, null);
	private static final NamespaceServerSideCode defaultCodeNull2 = new NamespaceServerSideCode(null, null, null);
	private static final NamespaceServerSideCode defaultCodeNull3 = new NamespaceServerSideCode("",   null, null);
	private static final NamespaceServerSideCode defaultCodeNull4 = new NamespaceServerSideCode("",   null, "");
	private static final NamespaceServerSideCode defaultCodeNull5 = new NamespaceServerSideCode("",   "a",  null);

	private String getUrl(NamespaceServerSideCode nssc) {
		return nssc.getUrl();
	}
	
	private String getPutTrigger(NamespaceServerSideCode nssc) {
		return nssc.getPutTrigger();
	}
	
	private String getRetrieveTrigger(NamespaceServerSideCode nssc) {
		return nssc.getRetrieveTrigger();
	}
	
	@Test
	public void testGetters() {
		String[][] testCases = {
			{urlCopy,             getUrl(defaultCode)},
			{putTriggerCopy,      getPutTrigger(defaultCode)},
			{retrieveTriggerCopy, getRetrieveTrigger(defaultCode)},
			
			{urlDiff,             getUrl(defaultCodeDiff)},
			{putTriggerDiff,      getPutTrigger(defaultCodeDiff)},
			{retrieveTriggerDiff, getRetrieveTrigger(defaultCodeDiff)},
			
			{null,                getUrl(defaultCodeNull1)},
			{null,                getPutTrigger(defaultCodeNull1)},
			{null,                getRetrieveTrigger(defaultCodeNull1)},
		};
		
		test_Getters(testCases);
	}

	@Test
	public void testHashCode() {
		checkHashCodeEquals(   defaultCode,      defaultCode);
		checkHashCodeEquals(   defaultCode,      defaultCodeCopy);
		checkHashCodeNotEquals(defaultCode,      defaultCodeDiff);
		checkHashCodeEquals(   defaultCode,      defaultCodeNull1);	// strictly we would want checkHashNotEquals(), but this is fine for now since hashCode doesn't need to be unique, and both are corner cases
		checkHashCodeEquals(   defaultCodeNull1, defaultCodeNull2);
	}
	
	@Test
	public void testEqualsObject() {
		NamespaceServerSideCode[][] testCases = {
			{defaultCode,      defaultCode,      defaultCodeDiff},
			{defaultCodeCopy,  defaultCode,      defaultCodeDiff},
			{defaultCodeDiff,  defaultCodeDiff,  defaultCode},
			{defaultCodeNull1, defaultCodeNull2, defaultCode},
		};
		test_FirstEqualsSecond_FirstNotEqualsThird(testCases);
		
		test_NotEquals(new Object[][]{
			{defaultCode, NamespaceOptions.templateOptions},
		});
	}

	@Test
	public void testToStringAndParse() {
		NamespaceServerSideCode[] testCases = {
			defaultCode,
			defaultCodeCopy,
			defaultCodeDiff,
			defaultCodeNull1,
			defaultCodeNull3,
			defaultCodeNull4,
			defaultCodeNull5,
		};
		
		for (NamespaceServerSideCode testCase : testCases)
			checkStringAndParse(testCase);
	}
	
	private void checkStringAndParse(NamespaceServerSideCode nssc) {
		assertEquals(nssc, NamespaceServerSideCode.parse( nssc.toString() ));
	}
	
}

package com.ms.silverking.cloud.dht;

import static com.ms.silverking.cloud.dht.NamespaceCreationOptions.Mode.OptionalAutoCreation_AllowMatches;
import static com.ms.silverking.cloud.dht.NamespaceCreationOptions.Mode.OptionalAutoCreation_DisallowMatches;
import static com.ms.silverking.cloud.dht.NamespaceCreationOptions.Mode.RequireAutoCreation;
import static com.ms.silverking.cloud.dht.NamespaceCreationOptions.Mode.RequireExplicitCreation;
import static com.ms.silverking.testing.Util.getTestMessage;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ms.silverking.cloud.dht.NamespaceCreationOptions.Mode;
import com.ms.silverking.cloud.dht.common.DHTConstants;

public class NamespaceCreationOptionsTest {

	private static final NamespaceCreationOptions defaultOptions           = NamespaceCreationOptions.defaultOptions;
	private static final NamespaceCreationOptions defaultOptionsCopy       = new NamespaceCreationOptions(Mode.OptionalAutoCreation_AllowMatches,    "^_.*", DHTConstants.defaultNamespaceOptions);
	private static final NamespaceCreationOptions defaultOptionsAlmostCopy = new NamespaceCreationOptions(Mode.OptionalAutoCreation_AllowMatches,    "^_.*", null);
	private static final NamespaceCreationOptions defaultOptionsDiff       = new NamespaceCreationOptions(Mode.OptionalAutoCreation_DisallowMatches, "",     DHTConstants.defaultNamespaceOptions);
	
	@Test
	public void testCanBeExplicitlyCreated() {
		Object[][] testCases = {
			{RequireExplicitCreation,              "", null,  true},
			{RequireAutoCreation,                  "", null, false},
			{OptionalAutoCreation_AllowMatches,    "", null,  true},
			{OptionalAutoCreation_DisallowMatches, "", null,  true},
		};
		
		for (Object[] testCase : testCases) {
			Mode mode                  =             (Mode)testCase[0];
			String regex               =           (String)testCase[1];
			NamespaceOptions nsOptions = (NamespaceOptions)testCase[2];
			boolean expected           =          (boolean)testCase[3];
			
			assertEquals(expected, createOptions(mode, regex, nsOptions).canBeExplicitlyCreated(""));
		}
	}

	@Test
	public void testCanBeAutoCreated() {
		Object[][] testCases = {
			{RequireExplicitCreation,              "",     null, "",     false},
			{RequireAutoCreation,                  "",     null, "",      true},
			{OptionalAutoCreation_AllowMatches,    "^_.*", null, "abc",  false},
			{OptionalAutoCreation_AllowMatches,    "^_.*", null, "_abc",  true},
			{OptionalAutoCreation_DisallowMatches, "^_.*", null, "abc",   true},
			{OptionalAutoCreation_DisallowMatches, "^_.*", null, "_abc", false},
		};
		
		for (Object[] testCase : testCases) {
			Mode mode                  =             (Mode)testCase[0];
			String regex               =           (String)testCase[1];
			NamespaceOptions nsOptions = (NamespaceOptions)testCase[2];
			String namespaceName       =           (String)testCase[3];
			boolean expected           =          (boolean)testCase[4];
			
			assertEquals(getTestMessage("canBeAutoCreated", mode, regex, nsOptions, namespaceName), expected, createOptions(mode, regex, nsOptions).canBeAutoCreated(namespaceName));
		}
	}
	
	private NamespaceCreationOptions createOptions(Mode m, String regex, NamespaceOptions options) {
		return new NamespaceCreationOptions(m, regex, options);
	}

//	@Test
	public void testToStringAndParse() {
		NamespaceCreationOptions[] testCases = {
//			defaultOptions,
//			defaultOptionsCopy,
//			defaultOptionsAlmostCopy,
//			defaultOptionsDiff,
		};
		
		for (NamespaceCreationOptions testCase : testCases)
			checkStringAndParse(testCase);
	}
	
	private void checkStringAndParse(NamespaceCreationOptions options) {
		assertEquals(options, NamespaceCreationOptions.parse( options.toString() ));
	}

}

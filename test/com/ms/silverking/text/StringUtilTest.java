package com.ms.silverking.text;

import static com.ms.silverking.testing.Util.createList;
import static com.ms.silverking.testing.Util.createToString;
import static com.ms.silverking.testing.Assert.exceptionNameChecker;
import static com.ms.silverking.testing.Util.getTestMessage;
import static com.ms.silverking.testing.Util.int_maxVal;
import static com.ms.silverking.testing.Util.long_maxVal;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.junit.Test;

import com.ms.silverking.numeric.MutableInteger;
import com.ms.silverking.testing.Util.ExceptionChecker;

public class StringUtilTest {

	@Test
	public void testSplitAndTrim() {
		String[][][] testCases = {
			{{"a"},                     {"a"},                              {"b"}, {"a"}},
			{{"	a"},                    {"a"},                              {"b"}, {"a"}},
			{{"a	b"},                {"a", "b"},                         {"b"}, {"a"}},
			{{"da	b cd ddd e    d "}, {"da", "b", "cd", "ddd", "e", "d"}, {"d"}, {"", "a	b c", "", "", "", "e"}},
		};
		
		for (String[][] testCase : testCases) {
			String     source      = testCase[0][0];
			String[] expected      = testCase[1];
			String     regex       = testCase[2][0];
			String[] expectedRegex = testCase[3];
			
			checkSplitAndTrim(source, expected, regex, expectedRegex);
		}
	}
	
	private void checkSplitAndTrim(String source, String[] expected, String regex, String[] expectedRegex) {
		String testName = "splitAndTrim";
		assertArrayEquals(getTestMessage(testName, source,        createToString(expected)),      expected,      StringUtil.splitAndTrim(source));             
		assertArrayEquals(getTestMessage(testName, source, regex, createToString(expectedRegex)), expectedRegex, StringUtil.splitAndTrim(source, regex));             
		assertArrayEquals(getTestMessage(testName, source,        createToString(expectedRegex)), expected,      StringUtil.splitAndTrimQuoted(source));             
	}

	@Test
	public void testToHexString() {
		Object[][] testCases = {
			{"0",                "00",                     0, 1, 1, 1, "00 ",                  "00 "},
			{"127",              "7f",                     0, 1, 1, 2, "7f ",                  "7f "},
			{"128",              "0080",                   0, 1, 1, 1, "00 ",                  "00 80 "},
			{"128",              "0080",                   1, 1, 1, 2, "80 ",                  "00 80:"},
			{"1000",             "03e8",                   0, 2, 1, 2, "03 e8:",               "03 e8:"},
			{""+int_maxVal,      "7fffffff:",              0, 4, 1, 6, "7f ff:ff:ff:",         "7f ff:ff:ff:"},
			{""+long_maxVal,     "7fffffff:ffffffff:",     0, 8, 2, 4, "7fff:ffff:ffff:ffff:", "7fff:ffff:ffff:ffff:"},
			{"1234"+long_maxVal, "029d73e6:cbe0c04f:ffff", 4, 4, 2, 5, "cbe0:c04f:",           "029d:73e6:cbe0 c04f:ffff:"},
		};
		
		for (Object[] testCase : testCases) {
			String val                = (String)testCase[0];
			String expected           = (String)testCase[1];
			int offset                =    (int)testCase[2];
			int length                =    (int)testCase[3];
			int minorGroupSize        =    (int)testCase[4];
			int majorGroupSize        =    (int)testCase[5];
			String expectedAll        = (String)testCase[6];
			String expectedByteBuffer = (String)testCase[7];
			
			checkToHexString(val, expected, offset, length, minorGroupSize, majorGroupSize, expectedAll, expectedByteBuffer);
		}
	}
	
	private void checkToHexString(String val, String expected, int offset, int length, int minorGroupSize, int majorGroupSize, String expectedAll, String expectedByteBuffer) {
		String testName = "toHexString";
		BigInteger bi = new BigInteger(val);
		byte[] _bytes = bi.toByteArray();
		ByteBuffer bb = ByteBuffer.wrap(_bytes);
		String valAsHex = bi.toString(16);
		
		assertEquals(getTestMessage(testName, val),                                                 expected,           StringUtil.toHexString(bi));             
		assertEquals(getTestMessage(testName, val, offset, length, minorGroupSize, majorGroupSize), expectedAll,        StringUtil.byteArrayToHexString(_bytes, offset, length, minorGroupSize, majorGroupSize));             
		assertEquals(getTestMessage(testName, val, minorGroupSize, majorGroupSize),                 expectedByteBuffer, StringUtil.byteBufferToHexString(bb, minorGroupSize, majorGroupSize));             
//		assertEquals(getTestMessage(testName, val),                                                 " ",                StringUtil.byteBufferToString(bb)); 
//		assertEquals(getTestMessage(testName, valAsHex),                                            bb,                 StringUtil.hexStringToByteBuffer(valAsHex));             
//		assertArrayEquals(getTestMessage(testName, valAsHex),                                       bb.array(),         StringUtil.hexStringToByteBuffer(valAsHex).array());
	}
	
	@Test
	public void testParseByte() {
		Object[][] testCases = {
//			{"-1", (byte)0},
//			{"1", (byte)0},
			{"00", (byte)0  },
			{"aC", (byte)172},
			{"Ff", (byte)255},
//			{"FG", (byte)255},
		};
		
		for (Object[] testCase : testCases) {
			String hexString = (String)testCase[0];
			byte expected    =   (byte)testCase[1];
			
			checkParseByte(hexString, expected);
		}
	}
	
	private void checkParseByte(String hexString, byte expected) {
		assertEquals(getTestMessage("parseByte", hexString), expected, StringUtil.parseByte(hexString));             
	}
	
	@Test
	public void testNextByteChars() {
		String intInHex = "00 80:";
		String bigHex   = "029d:73e6:cbe0 c04f:ffff:";
		
		Object[][] testCases = {
			{"  ",     0, null},
//			{"0",      0, "0"},
			{"00",     0, "00"},
//			{"FF",     0, "FF"},
			{intInHex, 0, "00"},	
			{intInHex, 1, "08"},
			{intInHex, 2, "80"},
			{bigHex,   2, "9d"},
			{bigHex,   3, "d7"},
			{bigHex,  13, "0c"},
			{bigHex,  14, "c0"},
			// what about bigHex with uppercase letters?
//			{bigHex,  23, "f"},
			{bigHex,  50, null},
			{"02   9d::73e6:cbe0 c04f:ffff:", 2, "9d"},
		};
		
		for (Object[] testCase : testCases) {
			String hexString = (String)testCase[0];
			int index        =    (int)testCase[1];
			String expected  = (String)testCase[2];
			
			checkNextByteChars(hexString, index, expected);
		}
	}
	
	private void checkNextByteChars(String hexString, int index, String expected) {
		assertEquals(getTestMessage("nextByteChars", hexString, index), expected, StringUtil.nextByteChars(hexString, new MutableInteger(index)));             
	}
	
	@Test
	public void testIsHexDigit() {
		Object[][] testCases = {
			{'0', true },
			{'5', true },
			{'9', true },
			{'a', true },
//			{'C', true },
//			{'F', true },
			{'G', false},
		};
		
		for (Object[] testCase : testCases) {
			char hexChar     =    (char)testCase[0];
			boolean expected = (boolean)testCase[1];
			
			checkIsHexDigit(hexChar, expected);
		}
	}
	
	private void checkIsHexDigit(char hexChar, boolean expected) {
		assertEquals(getTestMessage("isHexDigit", hexChar), expected, StringUtil.isHexDigit(hexChar));             
	}
	
	@Test
	public void testToString() {
		Object[][] testCases = {
			{new String[][]{{"a"}},                               "a ",             "\"a\" ",               "a "},
			{new String[][]{{"a", "b", "c"}},                     "a b c ",         "\"a\" \"b\" \"c\" ",   "a b c "},
			{new String[][]{{"a", "b", "c"}, {"1 ", "2", " 3"}},  "a b c 1  2  3 ", "\"1 \" \"2\" \" 3\" ", "1  2  3 "},
		};
		
		for (Object[] testCase : testCases) {
			String[][] array           = (String[][])testCase[0];
			String expectedArray       =     (String)testCase[1];
			String expectedArrayQuoted =     (String)testCase[2];
			String expectedList        =     (String)testCase[3];
			
			checkToString(array, expectedArray, expectedArrayQuoted, expectedList);
		}
	}
	
	private void checkToString(String[][] array2d, String expectedArray, String expectedArrayQuoted, String expectedList) {
		String testName = "toString";
		String[] lastArray = array2d[array2d.length-1];
		String array2dString   = createToString(array2d);
		String lastArrayString = createToString(lastArray);
		
		assertEquals(getTestMessage(testName,   array2dString), expectedArray,       StringUtil.arrayToString(array2d));             
		assertEquals(getTestMessage(testName, lastArrayString), expectedArrayQuoted, StringUtil.arrayToQuotedString(lastArray));             
		assertEquals(getTestMessage(testName, lastArrayString), expectedList,        StringUtil.listToString( createList(lastArray) ));             
		assertEquals(getTestMessage(testName, lastArrayString), expectedList,        StringUtil.toString( createList(lastArray) ));             
	}
	
	@Test
	public void testStartsWithIgnoreCase() {
		Object[][] testCases = {
			{"a",      "",      true },
			{"",       "a",     false},
			{"a",      "A",     true },
			{"aA",     "b",     false},
			{"aBc123", "ABC",   true },
			{"aBc123", "bc123", false},
		};
		
		for (Object[] testCase : testCases) {
			String target    =  (String)testCase[0];
			String value     =  (String)testCase[1];
			boolean expected = (boolean)testCase[2];
			
			checkStartsWithIgnoreCase(target, value, expected);
		}
	}
	
	private void checkStartsWithIgnoreCase(String target, String value, boolean expected) {
		assertEquals(getTestMessage("startsWithIgnoreCase", target, value), expected, StringUtil.startsWithIgnoreCase(target, value));             
	}
	
	@Test
	public void testMd5() {
		Object[][] testCases = {
			{"a",           "0cc175b9:c0f1b6a8:31c399e2:69772661:"},
			{"b",           "92eb5ffe:e6ae2fec:3ad71c77:7531578f:"},
			{"abc123",      "e99a18c4:28cb38d5:f2608536:78922e03:"},
			{"abracadabra", "ec5287c4:5f0e70ec:22d52e8b:cbeeb640:"},
		};
		
		for (Object[] testCase : testCases) {
			String input    =  (String)testCase[0];
			String expected =  (String)testCase[1];
			
			checkMd5(input, expected);
		}
	}
	
	private void checkMd5(String input, String expected) {
		assertEquals(getTestMessage("md5", input), expected, StringUtil.md5(input));             
	}
	
	@Test
	public void testCountOccurrences() {
		String abra = "123abracadabra321";
		
		Object[][] testCases = {
			{"",   ' ', 0},
			{" ",  ' ', 1},
			{"  ", ' ', 2},
			{"a",  'a', 1},
			{"a",  'b', 0},
			{abra, 'b', 2},
			{abra, 'c', 1},
			{abra, 'd', 1},
			{abra, 'e', 0},
		};
		
		for (Object[] testCase : testCases) {
			String text  = (String)testCase[0];
			char term    =   (char)testCase[1];
			int expected =    (int)testCase[2];
			
			checkCountOccurrences(text, term, expected);
		}
	}
	
	private void checkCountOccurrences(String text, char term, int expected) {
		assertEquals(getTestMessage("countOccurrences", text, term), expected, StringUtil.countOccurrences(text, term));             
	}
	
	@Test
	public void testReplicate() {
		Object[][] testCases = {
			{' ', "   ", "",    ""},
			{'5', "555", " ",   "   "},
			{'a', "aaa", "a",   "aaa"},
			{'Z', "ZZZ", "xYz", "xYzxYzxYz"},
		};
		
		for (Object[] testCase : testCases) {
			char charVal        =   (char)testCase[0];
			String charExpected = (String)testCase[1];
			String strVal       = (String)testCase[2];
			String strExpected  = (String)testCase[3];
			
			checkReplicate(charVal, charExpected, strVal, strExpected);
		}
	}
	
	private void checkReplicate(char charVal, String charExpected, String strVal, String strExpected) {
		String testName = "replicate";
		int n = 3;
		assertEquals(getTestMessage(testName, charVal), charExpected, StringUtil.replicate(charVal, n));             
		assertEquals(getTestMessage(testName,  strVal),  strExpected, StringUtil.replicate(strVal, n));             
	}
	
	@Test
	public void testProjectColumn() {
		String[] alpha = {"a 1 b 2 c 3", "x y z"};
		
		Object[][] testCases = {
			{new String[]{""}, 0, "",  true,  new String[]{""}},
			{new String[]{""}, 0, " ", true,  new String[]{""}},
			{new String[]{""}, 1, " ", true,  new String[]{}},
			{new String[]{""}, 1, " ", false, new String[]{null}},
			{alpha,            0, " ", true,  new String[]{"a", "x"}},
			{alpha,            4, " ", true,  new String[]{"c"}},
			{alpha,            4, " ", false, new String[]{"c", null}},
		};
		
		for (Object[] testCase : testCases) {
			String[] lines    = (String[])testCase[0];
			int column        =      (int)testCase[1];
			String regex      =   (String)testCase[2];
			boolean filter    =  (boolean)testCase[3];
			String[] expected = (String[])testCase[4];

			checkProjectColumn(lines, column, regex, filter, expected);
		}
	}
	
	private void checkProjectColumn(String[] lines, int column, String regex, boolean filter, String[] expected) {
		Object[] actual = StringUtil.projectColumn(createList(lines), column, regex, filter).toArray();
		assertArrayEquals(getTestMessage("projectColumn", createToString(lines), column, regex, filter, "expected = " + createList(expected), "actual = " + createList(actual)), expected, actual);
	}

	@Test
	public void testStripQuotes_Exceptions() {
		Object[][] testCases = {
			{"",    StringIndexOutOfBoundsException.class},
			{"'",   StringIndexOutOfBoundsException.class},
			{" \"",                RuntimeException.class},
			{"\" ",                RuntimeException.class},
			{"'b\"",               RuntimeException.class},
			{"\"b'",               RuntimeException.class},
			{"\"a''b\"\"c'",       RuntimeException.class},
		};
		
		for (Object[] testCase : testCases) {
			String quotedString             =   (String)testCase[0];
			Class<?> expectedExceptionClass = (Class<?>)testCase[1];
			
			String testMessage = getTestMessage("stripQuotes_Exceptions", testCase);  
			ExceptionChecker ec = new ExceptionChecker() { @Override public void check(){ checkStripQuotes(quotedString, null); } };
			exceptionNameChecker(ec, testMessage, expectedExceptionClass);
		}
	}
	
	@Test
	public void testStripQuotes() {
		Object[][] testCases = {
			{" ",                 " "},
			{"  ",                "  "},
			{"a",                 "a"},
			{"'b'",               "b"},
			{"\"b\"",             "b"},
			{"\"a''b\"\"c''d\"",  "a''b\"\"c''d"},
		};
		
		for (Object[] testCase : testCases) {
			String quotedString = (String)testCase[0];
			String expected     = (String)testCase[1];

			checkStripQuotes(quotedString, expected);
		}
	}
	
	private void checkStripQuotes(String quotedString, String expected) {
		assertEquals(getTestMessage("stripQuotes", quotedString), expected, StringUtil.stripQuotes(quotedString));
	}

}

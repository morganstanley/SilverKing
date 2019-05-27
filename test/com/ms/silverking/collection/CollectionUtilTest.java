package com.ms.silverking.collection;

import static com.ms.silverking.collection.CollectionUtil.defaultEmptyDef;
import static com.ms.silverking.collection.CollectionUtil.defaultEndBrace;
import static com.ms.silverking.collection.CollectionUtil.defaultMapEntrySeparator;
import static com.ms.silverking.collection.CollectionUtil.defaultMapString;
import static com.ms.silverking.collection.CollectionUtil.defaultSeparator;
import static com.ms.silverking.collection.CollectionUtil.defaultStartBrace;
import static com.ms.silverking.testing.Util.createSet;
import static com.ms.silverking.testing.Util.createList;
import static com.ms.silverking.testing.Util.createToString;
import static com.ms.silverking.collection.TestUtil.empty;
import static com.ms.silverking.testing.Util.getTestMessage;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

public class CollectionUtilTest {

	private static final String toStringName    = "toString";
	private static final String mapToStringName = "mapToString";
	private static final String stringSetName   = "stringSet";
	private static final String parseSetName    = "parseSet";
	
	private static final String start = defaultStartBrace;
	private static final String end   = defaultEndBrace;
	private static final char   sep   = defaultSeparator;
	private static final String mStr  = defaultMapString;
	private static final String mSep  = defaultMapEntrySeparator;
	
	@Test(expected=NullPointerException.class)
	public void testToString_Null() {
		CollectionUtil.toString(null);
	}
	
	@Test
	public void testToString() {
		assertEquals(toStringName, defaultEmptyDef, CollectionUtil.toString( createList() ));

		Object[][][] testCases = {
			{{empty},    {empty}      },
			{{"a"},      {"a"},       },
			{{"a", "b"}, {"a"+sep+"b"}},
			{{1},        {"1"},       },
			{{1, 2},     {"1"+sep+"2"}},
		};
		
		for (Object[][] testCase : testCases) {
			Object[] elements =         testCase[0];
			String expected   = (String)testCase[1][0];
			
			checkToString(elements, start+expected+end);
		}
	}
	
	private <T> void checkToString(T[] elements, String expected) {
		assertEquals( getTestMessage(toStringName, createToString(elements)), expected, CollectionUtil.toString( createList(elements) ));
	}
	
	@Test(expected=NullPointerException.class)
	public void testMapToString_Null() {
		CollectionUtil.mapToString(null);
	}
	
	@Test
	public void testMapToString() {
		Map<Integer, String> map = new HashMap<>();
		assertEquals(mapToStringName, defaultEmptyDef, CollectionUtil.mapToString(map));
		
		Object[][] testCases = {
			{1, empty, "1"+mStr},
			{1,   "a", "1"+mStr+"a"},
			{2, empty, "1"+mStr+"a"+mSep+"2"+mStr},
			{3,   "c", "1"+mStr+"a"+mSep+"2"+mStr+mSep+"3"+mStr+"c"},
		};
		
		for (Object[] testCase : testCases) {
			int key         =    (int)testCase[0];
			String value    = (String)testCase[1];
			String expected = (String)testCase[2];
			
			map.put(key, value);
			checkMapToString(map, start+expected+end);
		}
	}
	
	private <K, V> void checkMapToString(Map<K,V> map, String expected) {
		assertEquals( getTestMessage(mapToStringName, map),	expected, CollectionUtil.mapToString(map));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testStringSet() {
		Set[][] testCases = {
			{createSet(empty), createSet(empty)},
			{createSet("a"),   createSet("a")},
			{createSet(1),     createSet("1")},
			{createSet(1,2,3), createSet("1", "2", "3")},
		};
		
		for (Set[] testCase : testCases) {
			Set<String> param    = testCase[0];
			Set<String> expected = testCase[1];
			
			checkStringSet(param, expected);
		}
	}

	private <T> void checkStringSet(Set<T> param, Set<String> expected) {
		assertEquals( getTestMessage(stringSetName, param), expected, CollectionUtil.stringSet(param));
	}
	
	@Test
	public void testParseSet() {
		String[][][] testCases = {
			{{empty},     {empty},   {empty}},
			{{"{"},       {empty},   {empty}},
			{{"}"},       {empty},   {"}"}},
			{{"{}"},      {empty},   {empty}},
			{{"{a}"},     {empty},   {"a"}},
			{{"{a,b}"},     {","},   {"a", "b"}},
			{{"{a,b-c}"},   {","},   {"a", "b-c"}},
			{{"{a,b-c}"},   {"-"},   {"a,b", "c"}},
			{{"blah"},    {empty},   {"b", "l", "a", "h"}},
		};
		
		for (String[][] testCase : testCases) {
			String def        = testCase[0][0];
			String pattern    = testCase[1][0];
			String[] expected = testCase[2];
			
			checkParseSet(def, pattern, expected);
		}
	}

	private void checkParseSet(String def, String pattern, String[] expectedElements) {
		Set<String> expected = createSet(expectedElements);
		assertEquals( getTestMessage(parseSetName, def, pattern), expected, CollectionUtil.parseSet(def, pattern));
	}
}

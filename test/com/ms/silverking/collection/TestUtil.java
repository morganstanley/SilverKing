package com.ms.silverking.collection;

import static com.ms.silverking.testing.Util.createList;
import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class TestUtil {
	
	public static final int    expectedIntValue    = 0;
	public static final int    expectedIntValue2   = 1;
	public static final double expectedDoubleValue = 0.0;
	public static final String expectedStringValue = "0";
	public static final String empty = "";

	public static final Pair<Integer, Integer> pI              = new Pair<>(expectedIntValue,    expectedIntValue);
	public static final Pair<Double, Double>   pD              = new Pair<>(expectedDoubleValue, expectedDoubleValue);
	public static final Pair<String, String>   pS              = new Pair<>(expectedStringValue, expectedStringValue);
	public static final Pair<Integer, String> pIS              = new Pair<>(expectedIntValue,    expectedStringValue);
	public static final Pair<String, Integer> pSI              = new Pair<>(expectedStringValue, expectedIntValue);
	public static final Pair<Pair<Integer, String>, String> pM = new Pair<>(pIS,                 expectedStringValue);
	
	public static final int key1   = 1;
	public static final int value1 = 1;
	public static final int value2 = 2;
	
}

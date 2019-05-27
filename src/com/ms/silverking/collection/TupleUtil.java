package com.ms.silverking.collection;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.log.Log;

public class TupleUtil {
    static List<Object> parse(String def, String pattern, int expectedLength, String... typeNames) {
    	List<String>	rawList;
    	List<Object>	cookedList;
    	
    	def = def.trim();
    	rawList = ImmutableList.copyOf(def.split(pattern));
    	if (typeNames.length != expectedLength) {
    		Log.warningf("%d != %d", typeNames.length, expectedLength);
    		throw new RuntimeException("Expected length typeNames not provided");
    	}
    	if (rawList.size() != expectedLength) {
    		Log.warningf("%d != %d from %s", rawList.size(), expectedLength, def);
    		throw new RuntimeException("Expected length not met");
    	}
    	cookedList = new ArrayList<>(rawList.size());
    	for (int i = 0; i < typeNames.length; i++) {
    		String	typeName;
    		
    		typeName = typeNames[i];
    		if (typeName.equals(java.lang.String.class.getName())) {
    			cookedList.add(rawList.get(i));
    		} else if (typeName.equals(java.lang.Long.class.getName())) {
    			cookedList.add(Long.parseLong(rawList.get(i)));
    		} else if (typeName.equals(java.lang.Integer.class.getName())) {
    			cookedList.add(Integer.parseInt(rawList.get(i)));
    		} else if (typeName.equals(java.lang.Double.class.getName())) {
    			cookedList.add(Double.parseDouble(rawList.get(i)));
    		} else {
    			throw new RuntimeException("Unsupported type: "+ typeName);
    		}
    	}
    	return cookedList;
    }
}

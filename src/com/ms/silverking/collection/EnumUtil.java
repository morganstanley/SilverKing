package com.ms.silverking.collection;

public class EnumUtil {
	public static <T extends Enum<T>> T valueOfIgnoreCase(Class<T> e, String v) {
		for (T en : e.getEnumConstants()) {
			if (en.name().equalsIgnoreCase(v)) {
				return en;
			}
		}
		return null;
	}
	
	public static <T extends Enum<T>> T getEnumFromStringStart(Class<T> e, String v) {
		for (T en : e.getEnumConstants()) {
			//System.out.printf("\t\t%s %s %s\n", e, v, v.startsWith(en.name()));
			if (v.startsWith(en.name())) {
				return en;
			}
		}
		return null;
	}
}

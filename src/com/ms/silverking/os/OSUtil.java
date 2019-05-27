package com.ms.silverking.os;

public class OSUtil {
	public static boolean isWindows() {
		return System.getProperty("os.name").startsWith("Windows");
	}
}

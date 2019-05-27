package com.ms.silverking.util;


public class Libraries {	
	public static final String	useCustomGuavaProperty = Libraries.class.getName() + ".UseCustomGuava";
    public static final boolean	defaultUseCustomGuava = true;
	public static final boolean	useCustomGuava;
	
	static {
		useCustomGuava = PropertiesHelper.systemHelper.getBoolean(useCustomGuavaProperty, defaultUseCustomGuava);
	}
}

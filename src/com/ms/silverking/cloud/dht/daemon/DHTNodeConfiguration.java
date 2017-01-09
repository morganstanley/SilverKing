package com.ms.silverking.cloud.dht.daemon;

import com.ms.silverking.log.Log;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.UndefinedAction;

/**
 * Provide DHTNode specific configuration information. This is not used to specify configuration
 * information, but rather to read in configuration information that is provided elsewhere. 
 */
public class DHTNodeConfiguration {
	public static final String	dataBasePathProperty = DHTNodeConfiguration.class.getPackage().getName() + ".DataBasePath";
	// The below default path should not be used in a properly functioning system as the management
	// infrastructure should be setting the property on launch.
    public static final String  defaultDataBasePath = "/var/tmp/silverking/data";
	public static String	dataBasePath;
	
	static {
		String	def;
		
		def = PropertiesHelper.systemHelper.getString(dataBasePathProperty, UndefinedAction.ZeroOnUndefined);
		if (def != null && def.trim().length() != 0) {
			setDataBasePath(def.trim());
		} else {
			// Warn since a properly functioning system should have specified the property.
			Log.warning("No dataBasePathProperty specified. Using default.");
			setDataBasePath(defaultDataBasePath);
		}
	}
	
	public static void setDataBasePath(String _dataBasePath) {
		dataBasePath = _dataBasePath;
		Log.warning("dataBasePath: ", dataBasePath);
	}
}

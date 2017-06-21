package com.ms.silverking.cloud.gridconfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.ms.silverking.log.Log;
import com.ms.silverking.util.PropertiesHelper;

/**
 * Provides coherent, centralized configuration for grid settings.
 */
public class GridConfiguration implements Serializable  {
	private final String				   name;
	private final Map<String,String>	   envMap;
	
	public static final String	defaultBaseEnvVar = "GC_DEFAULT_BASE";
	static final String	defaultBaseProperty = GridConfiguration.class.getCanonicalName() +".DefaultBase";
	private static File	defaultBase;
	
	public static final String	envSuffixEnvVar = "GC_ENV_SUFFIX";
	static final String	envSuffixProperty = GridConfiguration.class.getCanonicalName() +".EnvSuffix";
	public static String	envSuffix;
	private static final String	defaultEnvSuffix = ".env";
	
	static {
		staticInit();
	}
	
	static void staticInit() {
		String	defaultBaseVal;
		String	envSuffixVal;
		
		defaultBaseVal = PropertiesHelper.systemHelper.getString(defaultBaseProperty, PropertiesHelper.UndefinedAction.ZeroOnUndefined);
		if (defaultBaseVal == null) {
			defaultBaseVal = PropertiesHelper.envHelper.getString(defaultBaseEnvVar, PropertiesHelper.UndefinedAction.ZeroOnUndefined);
			if (defaultBaseVal == null) {
				//System.err.printf("Property %s is undefined\n", defaultBaseProperty);
				//System.err.printf("Environment variable %s is undefined\n", defaultBaseEnvVar);
				//throw new RuntimeException("Unable to find GridConfig default base");
			} else {
				Log.info("GridConfiguration using defaultBase from environment variable");
			}
		} else {
			Log.info("GridConfiguration using defaultBase from property");
		}
		//if (defaultBaseVal.trim().length() == 0) {
			//throw new RuntimeException("GridConfiguration defaultBase is zero length");
		//}
		if (defaultBaseVal != null && defaultBaseVal.trim().length() > 0) {
			defaultBase = new File(defaultBaseVal);
		} else {
			defaultBase = null;
		}
		
		envSuffixVal = PropertiesHelper.systemHelper.getString(envSuffixProperty, PropertiesHelper.UndefinedAction.ZeroOnUndefined);
		if (envSuffixVal == null) {
			envSuffixVal = PropertiesHelper.envHelper.getString(envSuffixEnvVar, defaultEnvSuffix);
		}
		envSuffix = envSuffixVal;
	}
	
	public static File getDefaultBase() {
		if (defaultBase == null) {
			Log.warning("defaultBase is undefined");
			Log.warningf("Set env var %s or property %s", defaultBaseEnvVar, defaultBaseProperty);
			throw new RuntimeException("defaultBase is undefined");
		} else {
			return defaultBase;
		}
	}
	
	public GridConfiguration(String name, Map<String,String> envMap) {
		this.name = name;
		this.envMap = envMap;
	}
		
	public static GridConfiguration parseFile(File gcBase, String gcName) throws IOException {
		Map<String,String>	envMap;
		envMap = readEnvFile(new File(gcBase, gcName + envSuffix));
		return new GridConfiguration(gcName, envMap);
	}
	
	public static GridConfiguration parseFile(String gcName) throws IOException {
		return parseFile(getDefaultBase(), gcName);
	}
	
	public static Map<String, String> readEnvFile(File envFile) throws IOException {
		Map<String, String>	envMap;
		String	line;
		BufferedReader	reader;
		
		envMap = new HashMap<String, String>();
		reader = new BufferedReader(new InputStreamReader(new FileInputStream(envFile)));
		do {
			line = reader.readLine();
			if (line != null) {
				String[]	tokens;
				
				tokens = line.split("[\\s=]");
				if (tokens.length != 3 || !tokens[0].equals("export")) {
					Log.warning("Skipping bad env line: "+ tokens.length +" "+ line);
				} else {
					envMap.put(tokens[1], tokens[2]);
				}
			}
		} while (line != null);
		
		reader.close();
		return envMap;
	}
	
	public String getName() {
		return name;
	}
	
	public Map<String,String> getEnvMap() {
		return envMap;
	}
	
	public String toEnvString() {
		StringBuilder	sb;
		
		sb = new StringBuilder();
		for (Map.Entry<String, String> entry : envMap.entrySet()) {
			sb.append("export "+ entry.getKey() +"="+ entry.getValue() +"\n");
		}
		return sb.toString();
	}
	
	public String get(String envKey) {
		return envMap.get(envKey);
	}

	@Override
	public int hashCode() {
		return name.hashCode() ^ envMap.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		GridConfiguration other = (GridConfiguration)o;
		return name.equals(other.name) && envMap.equals(other.envMap);
	}
	
	@Override
	public String toString() {
		StringBuilder	sb;
		
		sb = new StringBuilder();
		sb.append(name);
		return sb.toString();
	}
}

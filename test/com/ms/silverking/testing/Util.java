package com.ms.silverking.testing;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;

public class Util {

	public interface ExceptionChecker {
		public void check();
	}
	
	public static final byte     byte_minVal =    Byte.MIN_VALUE;
	public static final byte     byte_maxVal =    Byte.MAX_VALUE;
	public static final long     long_minVal =    Long.MIN_VALUE;
	public static final long     long_maxVal =    Long.MAX_VALUE;
	public static final int       int_minVal = Integer.MIN_VALUE;
	public static final int       int_maxVal = Integer.MAX_VALUE;
	public static final double double_maxVal =  Double.MAX_VALUE;

	public static final double double_nan    = Double.NaN;
	public static final double double_negInf = Double.NEGATIVE_INFINITY;
	public static final double double_posInf = Double.POSITIVE_INFINITY;
	
	public static String getTestMessage(String testName, Object... params) {
		String message = "\nChecking " + testName + ":\n";
		for (Object param : params) {
			message += param + "\n";
		}
		
		return message;
	}
	
	@SafeVarargs
	public static <T> Set<T> createSet(T... elements) {
		return new HashSet<>( createList(elements) );
	}
	
	public static byte[] copy(byte[] a) {
		return copy(a, a.length);
	}
	
	public static byte[] copy(byte[] a, int length) {
		return Arrays.copyOf(a, length);
	}
	
	public static int[] copy(int[] a) {
		return copy(a, a.length);
	}
	
	public static int[] copy(int[] a, int length) {
		return Arrays.copyOf(a, length);
	}
	
	public static void sort(int[] a) {
		Arrays.sort(a);
	}

	@SafeVarargs
	public static <T> List<T> createList(T... elements) {
		return Arrays.asList(elements);
	}

//	if we get rid of all these usages of this method and inline them with the commented out portion that will work.
//  if we use this method with the commented out portion, we get these outputs instead of the actual contents of the array: [[I@579bb367] 
//	it has to do with primitives and generics:
//	   if we are using the method inline, we are calling it directly with the primitive array, like int[] a, so it works just fine
//	   if we are using this method, we are using a generic T, and passing createString(a), so it's getting a little confused
	@SafeVarargs
	public static <T> String createToString(T... elements) {
//		return Arrays.toString(elements);
		return Arrays.deepToString(elements);
	}
	
	// clone seems to work just fine with primitives and as long as it's just 1D
	public static byte[] toArray(byte... params) {
//		return Arrays.copyOf(params, params.length);
		return params.clone();
	}
	
	private static final String sep = File.separator;
	
	public static File getFile(Class<?> c, String testFilesDirName, String filename) {
		String resourceName = testFilesDirName;
		if (!filename.isEmpty())
			resourceName += sep + filename;
		return new File(c.getResource(resourceName).getPath());
	}
	
	public static void printEnv() {
		println("+++++++ENV");
		int count = 0;
    	for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
//    		System.out.println("found: " + entry.getKey() + " -> " + entry.getValue());
    		count++;
    	}
    	println(count);
	}
	
	public static void printProperties() {
		println("+++++++PROPERTIES");
		int count = 0;
    	for (Map.Entry<Object, Object> entry : System.getProperties().entrySet()) {
//    		System.out.println("found: " + entry.getKey() + " -> " + entry.getValue());
    		count++;
    	}
    	println(count);
	}
	
	public static void setEnv(String key, String value) {
		manipulateEnv(key, value, true);
	}

	public static void removeEnv(String key) {
		manipulateEnv(key, null, false);
	}

	// http://stackoverflow.com/questions/318239/how-do-i-set-environment-variables-from-java
	private static void manipulateEnv(String key, String value, boolean add) {
		try {
	        Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
	        Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
	        theEnvironmentField.setAccessible(true);
	        Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
            manipulate(env, key, value, add);
	        Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
	        theCaseInsensitiveEnvironmentField.setAccessible(true);
	        Map<String, String> ciEnv = (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
            manipulate(ciEnv, key, value, add);
	    }
	    catch (NoSuchFieldException e) {
			try {
			    Class[] classes = Collections.class.getDeclaredClasses();
			    Map<String, String> env = System.getenv();
			    for (Class cl : classes) {
			        if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
		        		Field field = cl.getDeclaredField("m");
			            field.setAccessible(true);
			            Object obj = field.get(env);
			            Map<String, String> map = (Map<String, String>) obj;
			            map.clear();
			            manipulate(map, key, value, add);
			        }
			    }
			} 
			catch (Exception e2) {
			    e2.printStackTrace();
			}
	    } 
		catch (Exception e1) {
	        e1.printStackTrace();
	    } 
	}
	
	private static void manipulate(Map<String, String> map, String key, String value, boolean add) {
        if (add) 
        	map.put(key, value);
        else
        	map.remove(key);
	}
	
	public static void runTests(Class<?> c) {
		println("OUTPUT:");
		Result result = JUnitCore.runClasses(c);
		printSummary(result);
	}
	
	private static void printSummary(Result result) {
		if (!result.wasSuccessful())
			println("\n\nERRORS:");
			
		for (Failure failure : result.getFailures()) {
//			System.out.println(failure.getMessage());
//			System.out.println(failure.getTestHeader());
//			System.out.println(failure.getDescription());
//			System.out.println(failure.getException());
			System.out.println(failure.getTestHeader());
			System.out.println(failure.getTrace());
//			System.out.println(failure.toString());
		}
		
		int run     = result.getRunCount();
		int failed  = result.getFailureCount();
		int ignored = result.getIgnoreCount();
		int passed  = run - failed;
		
		println("PASSED:  " + passed);
		println("FAILED:  " + failed);
		println("IGNORED: " + ignored);
		
		println("All passed?: " + String.valueOf( result.wasSuccessful() ).toUpperCase());
	}	

	public static void printName(String name) {
		println("**** " + name);
	}
	
	public static void println(String msg) {
		System.out.println(msg);
	}
	
	public static void println(int num) {
		println(num+"");
	}
	
	//////////////////////////////

	public static SKGridConfiguration getTestGridConfig() throws IOException {
		return SKGridConfiguration.parseFile( getTestGcName() );
	} 
	
	private static String getTestGcName() {
		return getEnvVariable("SK_GRID_CONFIG_NAME");
	}

	public static String getServers() {
		return getEnvVariable("SK_SERVERS");
	}
	
	public static String getServer1() {
		return getServer(0);
	}
	
	public static String getServer2() {
		return getServer(1);
	}
	
	private static String getServer(int num) {
		String servers = getServers();
		String[] serversSplit = servers.split(",");
		return serversSplit[num];
	}
	
	public static String getEnvVariable(String name) {
		String value = System.getenv(name);
		if (null == value)
			throw new RuntimeException(name + ": env variable undefined");
		
		return value;
	}
}

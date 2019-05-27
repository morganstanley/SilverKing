package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.io.IOException;

import com.google.common.base.Preconditions;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.io.FileUtil;
import com.ms.silverking.log.Log;

public class NamespacePropertiesIO {
    private static final String propertiesFileName = "properties";
    
    /*
     * Note that creationTime is not serialized by default for NamespaceProperties in order 
     * to ensure that DHT-stored namespace properties do not use this field which can vary by observer.
     * Instead, the DHT metadata is consulted for creation times. Here, however, we need to store the 
     * creation time, so it is done explicitly.   
     */
    
    private static File propertiesFile(File nsDir) {
        return new File(nsDir, propertiesFileName);
    }
    
    public static NamespaceProperties read(File nsDir) throws IOException {
    	try {
	        if (!nsDir.isDirectory()) {
	            throw new IOException("NamespacePropertiesIO.read() passed non-directory: "+ nsDir);
	        }
	        return _read(propertiesFile(nsDir));
    	} catch (IOException ioe) {
    		Log.logErrorWarning(ioe, "NamespacePropertiesIO.read() failed for: "+ nsDir);
    		throw ioe;
    	}
    }
    
    private static NamespaceProperties _read(File propertiesFile) throws IOException {
    	String	def;
    	int		index;
    	
    	def = FileUtil.readFileAsString(propertiesFile).trim();
    	index = def.lastIndexOf(',');
    	if (index < 0) {
    		throw new IOException("Failed to parse trailing creationTime from "+ def);
    	} else {
    		long	creationTime;
    		
    		Preconditions.checkArgument(index <= def.length() - 2);
    		creationTime = Long.parseLong(def.substring(index + 1));
    		return NamespaceProperties.parse(def.substring(0, index), creationTime);
    	}
    }
    
    public static void write(File nsDir, NamespaceProperties nsProperties) throws IOException {
    	_write(nsDir, nsProperties, false);
    }
    
    public static void rewrite(File nsDir, NamespaceProperties nsProperties) throws IOException {
    	_write(nsDir, nsProperties, true);
    }
    
    private static void _write(File nsDir, NamespaceProperties nsProperties, boolean allowRewrite) throws IOException {
        if (!nsDir.isDirectory()) {
            throw new IOException("NamespacePropertiesIO.write() passed non-directory: "+ nsDir);
        }
        if (allowRewrite && propertiesFileExists(nsDir)) {
        	propertiesFile(nsDir).delete();
        }
        if (propertiesFileExists(nsDir)) {
            NamespaceProperties existingProperties;
            
            existingProperties = read(nsDir);
            if (!nsProperties.equals(existingProperties)) {
                System.err.println(nsProperties);
                System.err.println(existingProperties);
                System.err.println();
                System.err.flush();
                System.out.println(nsProperties);
                System.out.println(existingProperties);
                System.out.println();
                System.out.flush();
                nsProperties.debugEquals(existingProperties);
                throw new RuntimeException("Existing properties != nsProperties");
            }
        } else {
            _write(propertiesFile(nsDir), nsProperties);
        }
    }
    
    private static void _write(File propertiesFile, NamespaceProperties nsProperties) throws IOException {
        FileUtil.writeToFile(propertiesFile, nsProperties.toString() +","+ nsProperties.getCreationTime());
    }
    
    public static boolean propertiesFileExists(File nsDir) throws IOException {
        return propertiesFile(nsDir).exists();
    }
}

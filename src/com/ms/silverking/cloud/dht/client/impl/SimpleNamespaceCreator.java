package com.ms.silverking.cloud.dht.client.impl;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.ms.silverking.cloud.dht.common.Namespace;

public class SimpleNamespaceCreator implements NamespaceCreator {    
    public SimpleNamespaceCreator() {
    }
    
    @Override
    public Namespace createNamespace(String namespace) {
        try {
            MessageDigest   md;
            byte[]          bytes;
            
            bytes = namespace.getBytes();
            // FUTURE - think about speeding this up
            // by using a thread-local digest
            // like MD5KeyDigest
            md = MessageDigest.getInstance("MD5");
            md.update(bytes, 0, bytes.length);
            return new SimpleNamespace(md.digest());
        } catch (NoSuchAlgorithmException nsae) {
            throw new RuntimeException("panic");
        }
    }
    
    public static void main(String[] args) {
    	for (String arg : args) {
    		Namespace	ns;
    		
    		ns = new SimpleNamespaceCreator().createNamespace(arg);
    		System.out.printf("%s\t%x\t%d\n", arg, ns.contextAsLong(), ns.contextAsLong());
    	}
    }
}

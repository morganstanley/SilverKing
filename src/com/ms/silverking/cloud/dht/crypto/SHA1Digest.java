package com.ms.silverking.cloud.dht.crypto;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SHA1Digest {
    private static ThreadLocal<MessageDigest>  tl = new ThreadLocal<MessageDigest>();
    
    public static final int BYTES = 20; 
    
    public static MessageDigest getLocalMessageDigest() {
        MessageDigest   md;
        
        // FUTURE - ADD CODE FOR LWT THREADS TO GET THIS WITHOUT THE TL LOOKUP
        // possibly a factory that we pass to LWT to generate our threads
        md = tl.get();
        if (md == null) {
            try {
                md = MessageDigest.getInstance("SHA-1");
                tl.set(md);
            } catch (NoSuchAlgorithmException nsae) {
                throw new RuntimeException("panic");
            }
        }
        md.reset();
        return md;
    }
}

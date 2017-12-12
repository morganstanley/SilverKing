package com.ms.silverking.cloud.dht.client.example;

import java.io.IOException;

import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.client.AsyncInvalidation;
import com.ms.silverking.cloud.dht.client.AsyncPut;
import com.ms.silverking.cloud.dht.client.AsyncSingleValueRetrieval;
import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;

public class AsyncInvalidationExample {    
	private static final String	key = "key";
	private static final String	value = "value";

	public static String runExample(SKGridConfiguration gridConfig) {
		try {
            AsynchronousNamespacePerspective<String, String>    asyncNSP;
            AsyncInvalidation<String>	asyncInvalidation;
            AsyncPut<String>    asyncPut;
            AsyncSingleValueRetrieval<String,String>    asyncGet;
            DHTSession	session;
            Namespace	ns;
            
            session = new DHTClient().openSession(gridConfig);
            ns = session.createNamespace("MyNamespace" + System.currentTimeMillis(), session.getDefaultNamespaceOptions().versionMode(NamespaceVersionMode.SYSTEM_TIME_NANOS));
            System.out.printf("Using namespace %s\n", ns.getName());
            asyncNSP = ns.openAsyncPerspective(String.class, String.class);
            
            asyncPut = asyncNSP.put(key, value);
            asyncPut.waitForCompletion();
                        
            asyncPut = asyncNSP.put(key, value);
            asyncPut.waitForCompletion();
            
            asyncGet = asyncNSP.get(key);
            asyncGet.waitForCompletion();            
            System.out.printf("Before invalidation %s\n", asyncGet.getValue());
            
            asyncInvalidation = asyncNSP.invalidate(key);
            asyncInvalidation.waitForCompletion();
            
            asyncGet = asyncNSP.get(key);
            asyncGet.waitForCompletion();
            System.out.printf("After invalidation %s\n", asyncGet.getValue());
            return asyncGet.getValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
	}
	
    public static void main(String[] args) throws IOException {
    	System.out.println( runExample( SKGridConfiguration.parseFile(args[0]) ) );
    }
}

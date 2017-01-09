package com.ms.silverking.cloud.dht.client.example;

import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.EmbeddedSK;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;

public class HelloEmbeddedDHT2 {
	
	public static String test() {
		try {
			SynchronousNamespacePerspective<String, String> syncNSP;
			ClientDHTConfiguration							dhtConfig;
      
			System.out.println("Creating embedded SK instance");
			dhtConfig = EmbeddedSK.createEmbeddedSKInstance();
			System.out.println("Embedded SK instance running at: "+ dhtConfig);
  
			syncNSP = new DHTClient().openSession(dhtConfig)
					.openSyncNamespacePerspective("_MyNamespace", String.class, String.class);
			syncNSP.put("Hello", "world!");
			return syncNSP.get("Hello");
		} catch (Exception e) {
			e.printStackTrace();
		}
  
		return "<error: shouldn't have reached here>";	
	}
	
	public static void main(String[] args) {
		System.out.println( test() );
		System.exit(0);
	}
}

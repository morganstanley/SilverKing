package com.ms.silverking.cloud.skfs.management;

import java.io.IOException;
import java.util.Set;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.SessionOptions;
import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.NamespaceCreationException;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.log.Log;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

public class SKFSNamespaceCreator {
	private final DHTClient		client;
	private final DHTSession	session;
	
	public SKFSNamespaceCreator(ClientDHTConfiguration dhtConfig, String preferredServer) throws IOException, ClientException {
		client = new DHTClient();
		session = client.openSession(new SessionOptions(dhtConfig, preferredServer));
	}
	
	public void createNamespaces(Set<String> namespaces, NamespaceOptions nsOptions) throws NamespaceCreationException {
		Log.warning("Creating: ", CollectionUtil.toString(namespaces));
		Log.warning("nsOptions: ", nsOptions);
		for (String namespace : namespaces) {
			createNamespace(namespace, nsOptions);
		}
	}
	
	public void createNamespace(String namespace, NamespaceOptions nsOptions) throws NamespaceCreationException {
		Stopwatch	sw;
		
		sw = new SimpleStopwatch();
		session.createNamespace(namespace, nsOptions);
		sw.stop();
		Log.warning("Created namespace: "+ namespace +"\tElapsed: "+ sw.getElapsedSeconds());
	}
	
	public void close() {
		session.close();
	}
}

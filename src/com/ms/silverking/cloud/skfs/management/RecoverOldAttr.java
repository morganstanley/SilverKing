package com.ms.silverking.cloud.skfs.management;

import java.io.IOException;

import com.ms.silverking.cloud.dht.GetOptions;
import com.ms.silverking.cloud.dht.NonExistenceResponse;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.StoredValue;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;

public class RecoverOldAttr {
	private final SynchronousNamespacePerspective<String, byte[]>	syncNSP;
	
	private static final String	attrNamespace = "attr";
	
	public RecoverOldAttr(SKGridConfiguration gc) throws ClientException, IOException {
		DHTClient		client;
		DHTSession		session;
		
		client = new DHTClient();
		session = client.openSession(gc);
		syncNSP = session.openSyncNamespacePerspective(attrNamespace, String.class, byte[].class);
	}
	
	public void recover(String file, long version) throws PutException, RetrievalException {
		StoredValue<byte[]>	oldAttrValue;
		GetOptions	getOptions;
		
		getOptions = syncNSP.getOptions().getDefaultGetOptions().versionConstraint(VersionConstraint.exactMatch(version)).nonExistenceResponse(NonExistenceResponse.NULL_VALUE);
		oldAttrValue = syncNSP.get(file, getOptions);
		if (oldAttrValue == null || oldAttrValue.getValue() == null) {
			System.out.printf("Couldn't find file %s version %d\n", file, version);
		} else {
			System.out.printf("Rewriting file %s version %d\n", file, version);
			syncNSP.put(file, oldAttrValue.getValue());
		}
	}

	public static void main(String[] args) {
		if (args.length != 3) {
			System.out.println("args: <gridConfig> <file> <version>");
		} else {
			try {
				RecoverOldAttr		roa;
				SKGridConfiguration	gc;
				String				file;
				long				version;
				
				gc = SKGridConfiguration.parseFile(args[0]);
				file = args[1];
				version = Long.parseLong(args[2]);
				roa = new RecoverOldAttr(gc);
				roa.recover(file, version);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}

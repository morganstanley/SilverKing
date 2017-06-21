package com.ms.silverking.cloud.skfs.management;

import java.io.IOException;

import com.ms.silverking.cloud.dht.GetOptions;
import com.ms.silverking.cloud.dht.NonExistenceResponse;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.StoredValue;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.skfs.dir.FileAttr;
import com.ms.silverking.cloud.skfs.dir.FileID;

public class GetFileAttributes {
	private final SynchronousNamespacePerspective<String, byte[]>	syncNSP;
	
	private static final String	attrNamespace = "attr";
	
	public GetFileAttributes(SKGridConfiguration gc) throws ClientException, IOException {
		DHTClient		client;
		DHTSession		session;
		
		client = new DHTClient();
		session = client.openSession(gc);
		syncNSP = session.openSyncNamespacePerspective(attrNamespace, String.class, byte[].class);
	}
	
	public void search(String file) throws RetrievalException {
		long	version;
		
		version = Long.MAX_VALUE;
		do {
			version = findPrevAttr(file, version) - 1;
		} while (version > 0);
	}
	
	public long findPrevAttr(String file, long maxVersion) throws RetrievalException {
		StoredValue<byte[]>	storedAttr;
		GetOptions	getOptions;
		
		getOptions = syncNSP.getOptions().getDefaultGetOptions().versionConstraint(VersionConstraint.maxBelowOrEqual(maxVersion)).nonExistenceResponse(NonExistenceResponse.NULL_VALUE).retrievalType(RetrievalType.VALUE_AND_META_DATA);
		storedAttr = syncNSP.get(file, getOptions);
		if (storedAttr == null || storedAttr.getValue() == null) {
			return 0;
		} else {
			displayAttr(storedAttr);
			return storedAttr.getVersion();
		}
	}

	private void displayAttr(StoredValue<byte[]> storedAttr) {
		FileAttr	attr;
		FileID		fid;
		
		attr = FileAttr.deserialize(storedAttr.getValue());
		fid = attr.getFileID();
		System.out.printf("%s\n", fid.toString());
	}

	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("args: <gridConfig> <file>");
		} else {
			try {
				GetFileAttributes	roa;
				SKGridConfiguration	gc;
				String				file;
				long				version;
				
				gc = SKGridConfiguration.parseFile(args[0]);
				file = args[1];
				roa = new GetFileAttributes(gc);
				roa.search(file);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}

package com.ms.silverking.cloud.skfs.dir;

import java.io.IOException;

import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.text.StringUtil;

public class DirReader {
	private SynchronousNamespacePerspective<String,byte[]>	dirNSP;
	
	private static final String	dirNamespaceName = "dir";
	
	public DirReader(SKGridConfiguration gc) throws IOException, ClientException {
		DHTClient	dhtClient;
		
		dhtClient = new DHTClient();
		dirNSP = dhtClient.openSession(gc).openSyncNamespacePerspective(dirNamespaceName);
	}
	
	public byte[] readDir(String dirName) {
		try {
			return dirNSP.get(dirName);
		} catch (RetrievalException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public void displayDir(String dirName) {
		byte[]		rawDir;
		Directory	dir;
		
		rawDir = readDir(dirName);
		System.out.printf("%s\t%d\n\n", dirName, rawDir.length);
		
		dir = new DirectoryInPlace(rawDir, 0, rawDir.length);		
		for (int i = 0; i < dir.getNumEntries(); i++) {
			System.out.printf("%d\t%s\n", i, dir.getEntry(i));
		}
		System.out.println();
		
		DirectoryInMemory	dirInMem;
		
		dirInMem = new DirectoryInMemory((DirectoryInPlace)dir);
		for (int i = 0; i < dir.getNumEntries(); i++) {
			System.out.printf("%d\t%s\n", i, dir.getEntry(i));
		}
		System.out.println();
		
		System.out.println("serialize");
		byte[]		d;
		d = dirInMem.serialize();
		System.out.printf("%s\n", StringUtil.byteArrayToHexString(rawDir));
		//System.out.printf("%s\n", StringUtil.byteArrayToHexString(d));
		System.out.println("serialization complete");
		dir = new DirectoryInPlace(d, 0, d.length);		
		for (int i = 0; i < dir.getNumEntries(); i++) {
			System.out.printf("%d\t%s\n", i, dir.getEntry(i));
		}
	}

	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("<gridConfig> <dir>");
		} else {
			try {
				DirReader			dirReader;
				SKGridConfiguration	gc;
				String				dirName;
				
				gc = SKGridConfiguration.parseFile(args[0]);
				dirName = args[1];
				dirReader = new DirReader(gc);
				dirReader.displayDir(dirName);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}

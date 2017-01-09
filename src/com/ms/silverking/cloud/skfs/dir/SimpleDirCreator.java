package com.ms.silverking.cloud.skfs.dir;

import java.io.File;
import java.io.IOException;

import com.ms.silverking.cloud.dht.NamespacePerspectiveOptions;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;

public class SimpleDirCreator {
	private final DHTClient		dhtClient;
	private final DHTSession	session;
	private final SynchronousNamespacePerspective<String,byte[]>	attrNSP;
	private final SynchronousNamespacePerspective<String,byte[]>	dirNSP;
	
	private static final String	attrNSName = "attr";
	private static final String	dirNSName = "dir";
	
	public SimpleDirCreator(SKGridConfiguration gridConfig) throws ClientException, IOException {
		dhtClient = new DHTClient();
		session = dhtClient.openSession(gridConfig);
		dirNSP = session.openSyncNamespacePerspective(dirNSName, String.class, byte[].class);
		attrNSP = session.openSyncNamespacePerspective(attrNSName, String.class, byte[].class);
	}
	
	public void create(File inputFile, String outputDir) throws IOException, PutException {
		create(SimpleDir.readFromFile(inputFile), outputDir);
	}
	
	public void create(SimpleDir simpleDir, String outputDir) throws IOException, PutException {
		RawSimpleDir	rawSimpleDir;
		byte[]			simpleDirBytes;
		FileAttr		fa;
		
		rawSimpleDir = simpleDir.toRawSimpleDir();
		simpleDirBytes = rawSimpleDir.serialize();
		dirNSP.put(outputDir, simpleDirBytes);
		fa = new FileAttr(FileID.generateSKFSFileID(), Stat.createDirStat(0777, simpleDirBytes.length));
		attrNSP.put(outputDir, fa.serialize());
	}
	
	public static void main(String[] args) {
		if (args.length != 3) {
			System.err.printf("args: <gridConfig> <inputFile> <outputDir>\n");
		} else {
			try {
				File	inputFile;
				String	outputDir;
				SKGridConfiguration	gc;
				
				gc = SKGridConfiguration.parseFile(args[0]);
				inputFile = new File(args[1]);
				outputDir = args[2];
				new SimpleDirCreator(gc).create(inputFile, outputDir);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}

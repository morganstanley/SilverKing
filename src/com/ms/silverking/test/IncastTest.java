package com.ms.silverking.test;


import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.NamespacePerspectiveOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.SessionOptions;
import com.ms.silverking.cloud.dht.StorageType;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.log.Log;
import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.thread.lwt.DefaultWorkPoolParameters;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

// CONSIDER USING BULK THROUGHPUT ANALYSIS INSTEAD OF THIS CLASS

public class IncastTest {		
	private final SynchronousNamespacePerspective<Integer, byte[]>	syncNSP;
	private final String	ns;
	
	private static final String	nsBase = "Incast_";
	
	private enum Mode	{Write, Read};
	
	private static final boolean   verbose = false;

	/*
    private static ClientDHTConfiguration getDHTConfiguration(String gridConfigName) {
        try {
            GridConfiguration       gridConfig;
            ClientDHTConfiguration  clientDHTConfiguration;
            
            gridConfig = SKGridConfiguration.parseFile(gridConfigName);
            clientDHTConfiguration = new ClientDHTConfiguration(gridConfig.getName());
            return clientDHTConfiguration;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
    */
	
	public IncastTest(String gridConfig, String host, String id) throws ClientException, IOException {
	    DHTSession session;
	    
		ns = nsBase + id; 
		Log.warning("Namespace: "+ ns);
        session = new DHTClient().openSession(new SessionOptions(SKGridConfiguration.parseFile(gridConfig), host));
        
        session.getDefaultNamespaceOptions()
                .storageType(StorageType.RAM)
                .consistencyProtocol(ConsistencyProtocol.LOOSE)
                .versionMode(NamespaceVersionMode.SYSTEM_TIME_MILLIS)
                .defaultPutOptions(
                        session.getDefaultPutOptions()
                        .compression(Compression.NONE)
                        .checksumType(ChecksumType.NONE));
        
		syncNSP = session.openSyncNamespacePerspective(ns, Integer.class, byte[].class);
        //syncNSP = new DHTClient().openSession(new SessionOptions(getDHTConfiguration(gridConfig), host))
        //        .openSyncNamespacePerspective(ns, nspOptions);
	}
	
	public long write(int numKeys, int batchSize, int valueSize) throws PutException {
		int		keysBatched;
		long	totalBytes;
		int		keyIndex;
		
		Log.warning("Write test");
		Log.warning("numKeys:\t" + numKeys +"\tbatchSize:\t"+ batchSize +"\tvalueSize:\t"+ valueSize);
		keysBatched = 0;
		totalBytes = 0;
		keyIndex = 0;
		while (keysBatched < numKeys) {
			Map<Integer, byte[]>	batch;
			int	thisBatchSize;

			thisBatchSize = Math.min(batchSize, numKeys - keysBatched);
			batch = new HashMap<>(thisBatchSize);
			for (int i = 0; i < thisBatchSize; i++) {
				Integer	key;
				byte[]	value;
				byte[]	keyBytes;
				
				key = new Integer(keyIndex++);
				keyBytes = NumConversion.intToBytes(key.intValue());
				value = new byte[valueSize];
				System.arraycopy(keyBytes, 0, value, 0, keyBytes.length);
				batch.put(key, value);
				totalBytes += value.length;
			}
			keysBatched += thisBatchSize;

			if (verbose) {
			    Log.warning("Writing batch:\t"+ thisBatchSize +"\t"+ keysBatched);
			}
			syncNSP.put(batch);
            if (verbose) {
                Log.warning("Done:         \t"+ thisBatchSize +"\t"+ keysBatched +"\t"+ totalBytes);
            }
		}
		return totalBytes;
	}
	
	public long read(int numKeys, int batchSize) throws RetrievalException {
		int		keysBatched;
		long	totalBytes;
		int		keyIndex;
		
		Log.warning("Read test");
		Log.warning("numKeys:\t" + numKeys +"\tbatchSize:\t"+ batchSize);
		totalBytes = 0;
		keysBatched = 0;
		keyIndex = 0;
		while (keysBatched < numKeys) {
			Set<Integer>         batchKeys;
			Map<Integer, byte[]> batch;
			int					 thisBatchSize;

			thisBatchSize = Math.min(batchSize, numKeys - keysBatched);
			batchKeys = new HashSet<>(thisBatchSize);
			for (int i = 0; i < thisBatchSize; i++) {
				batchKeys.add(new Integer(keyIndex++));
			}
			keysBatched += thisBatchSize;
			
			if (verbose) {
			    Log.warning("Reading batch:\t"+ thisBatchSize +"\t"+ keysBatched);
			}
			batch = syncNSP.get(batchKeys);
			if (batch.size() != batchKeys.size()) {
				Log.warning("batch.size() != batchKeys.size()");
			}
			for (byte[] value : batch.values()) {
				if (value != null) {
					totalBytes += value.length;
				} else {
					Log.warning("Null value!");
				}
			}
            if (verbose) {
                Log.warning("Done:         \t"+ thisBatchSize +"\t"+ keysBatched +"\t"+ totalBytes);
            }
		}
		return totalBytes;
	}
	
	
	public static void main(String[] args) {
    	try {
    		IncastTest	incastTest;
    		String	gridConfig;
    		String	host;
    		String	id;
    		Mode	mode;
    		int		numKeys;
    		int		valueSize;
    		int		batchSize;
    		Stopwatch	sw;
    		long		totalBytes;
    		double		Bps;
    		double		bps;
    		double		Mbps;
    		double		MBps;
    		
    		if (args.length < 6 || args.length > 7) {
    		    System.out.println("You should probably be using BulkThroughputAnalysis in place of this");
    			System.out.println("args: <gridConfig> <host> <id> Write <numKeys> <batchSize>");
    			System.out.println("args: <gridConfig> <host> <id> Read <numKeys> <batchSize> <valueSize>");
    			return;
    		}
    		gridConfig = args[0];
    		host = args[1];
    		id = args[2];
    		mode = Mode.valueOf(args[3]);
    		numKeys = Integer.parseInt(args[4]);
    		batchSize = Integer.parseInt(args[5]);
            LWTPoolProvider.createDefaultWorkPools(DefaultWorkPoolParameters.defaultParameters());
			incastTest = new IncastTest(gridConfig, host, id);
			sw = new SimpleStopwatch();
    		switch (mode) {
    		case Write: 
    			valueSize = Integer.parseInt(args[6]);
    			totalBytes = incastTest.write(numKeys, batchSize, valueSize);
    			break;
    		case Read: 
    			totalBytes = incastTest.read(numKeys, batchSize);
    			break;
    		default: throw new RuntimeException("panic");
    		}
    		sw.stop();
    		
    		Bps = (double)totalBytes / sw.getElapsedSeconds();
    		bps = Bps * 8.0;
    		Mbps = bps / 1e6;
    		MBps = Bps / 1e6;
    		
    		Log.warning("Elapsed:\t"+ sw.getElapsedSeconds());
    		Log.warning("totalBytes:\t"+ totalBytes);
    		Log.warning("bps:\t"+ bps);
    		Log.warning("Bps:\t"+ Bps);
    		Log.warning("Mbps:\t"+ Mbps);
    		Log.warning("MBps:\t"+ MBps);
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
}

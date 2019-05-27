package com.ms.silverking.cloud.dht.client.apps;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespacePerspectiveOptions;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.SessionOptions;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.WaitMode;
import com.ms.silverking.cloud.dht.WaitOptions;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.OperationException;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.SnapshotException;
import com.ms.silverking.cloud.dht.client.StoredValue;
import com.ms.silverking.cloud.dht.client.SyncRequestException;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.common.DHTUtil;
import com.ms.silverking.cloud.dht.common.OptionsHelper;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.log.Log;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.thread.lwt.DefaultWorkPoolParameters;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

public class ClientTool {
	private final DHTClient	dhtClient;
	
	private static final int	warmupDelayMillis = 500;
	
	private static final int   clientWorkUnit = 10;
	
	private static final String    multiKeyDelimiter = ":";
    private static final String    noSuchValue = "No such value";
	
	public ClientTool() throws IOException {
		dhtClient = new DHTClient();
	}
	
	private void doWaitFor(ClientOptions options, SynchronousNamespacePerspective<String,byte[]> syncNSP, Stopwatch sw) throws OperationException, IOException {
		SynchronousNamespacePerspective<String,byte[]>	ns;
		WaitOptions			waitOptions;
		StoredValue<byte[]>	storedValue;
		
        waitOptions = OptionsHelper.newWaitOptions(RetrievalType.VALUE);
		if (options.timeoutSeconds != 0) {
	        waitOptions = waitOptions.timeoutSeconds(options.timeoutSeconds).timeoutResponse(options.timeoutResponse);
		}
		
		Log.warning("Getting value...");
		storedValue = null;
		sw.reset();
		for (int i = 0; i < options.reps; i++) {
			storedValue = syncNSP.waitFor(options.key, waitOptions);
		}
		sw.stop();
		if (storedValue == null) {
			Log.warning(noSuchValue);
		} else {
			Log.warning(new String(storedValue.getValue()));
		}
	}
	
    private void doMultiGet(ClientOptions options, SynchronousNamespacePerspective<String,byte[]> syncNSP, Stopwatch sw) throws OperationException, IOException {
        Map<String, ? extends StoredValue<byte[]>>  storedValues;
        
        storedValues = doMultiRetrieve(options, syncNSP, sw, RetrievalType.VALUE, WaitMode.GET);
        displayValueMap(storedValues);
    }
    
	private void displayValueMap(Map<String, ? extends StoredValue<byte[]>> storedValues) {
	    for (Map.Entry<String, ? extends StoredValue<byte[]>> entry : storedValues.entrySet()) {
	        StoredValue<byte[]>    storedValue;
	        
	        storedValue = entry.getValue();
	        System.out.printf("%10s => %s\n", entry.getKey(), storedValue != null ? new String(storedValue.getValue()) : noSuchValue);
	    }
    }

    private void doGet(ClientOptions options, SynchronousNamespacePerspective<String,byte[]> syncNSP, Stopwatch sw) throws OperationException, IOException {
		StoredValue<byte[]>	storedValue;
		
        storedValue = doRetrieve(options, syncNSP, sw, RetrievalType.VALUE, WaitMode.GET);
		if (storedValue == null) {
			Log.warning(noSuchValue);
		} else {
		    System.out.println(new String(storedValue.getValue()));
		}
	}
	
    private void doGetMeta(ClientOptions options, SynchronousNamespacePerspective<String, byte[]> syncNSP, Stopwatch sw)
            throws OperationException, IOException {
        StoredValue<byte[]> storedValue;
        
        storedValue = doRetrieve(options, syncNSP, sw, RetrievalType.META_DATA, WaitMode.GET);
        if (storedValue == null) {
            Log.warning(noSuchValue);
        } else {
            System.out.println(storedValue.getMetaData().toString(true));
        }
    }
    
    private void doGetValueAndMeta(ClientOptions options, SynchronousNamespacePerspective<String, byte[]> syncNSP, Stopwatch sw)
            throws OperationException, IOException {
        StoredValue<byte[]> storedValue;
        
        storedValue = doRetrieve(options, syncNSP, sw, RetrievalType.VALUE_AND_META_DATA, WaitMode.GET);
        if (storedValue == null) {
            Log.warning(noSuchValue);
        } else {
            System.out.println(new String(storedValue.getValue()));
            System.out.println(storedValue.getMetaData().toString(true));
        }
    }
    
    private StoredValue<byte[]> doRetrieve(ClientOptions options, SynchronousNamespacePerspective<String,byte[]> syncNSP, Stopwatch sw,
                            RetrievalType retrievalType, WaitMode waitMode)
            throws OperationException, IOException {
        return (StoredValue<byte[]>)_doRetrieve(options, syncNSP, sw, retrievalType, waitMode);
    }

    private Map<String, ? extends StoredValue<byte[]>> doMultiRetrieve(ClientOptions options,
            SynchronousNamespacePerspective<String, byte[]> syncNSP, Stopwatch sw, RetrievalType retrievalType,
            WaitMode waitMode) throws OperationException, IOException {
        return (Map<String, ? extends StoredValue<byte[]>>)_doRetrieve(options, syncNSP, sw, retrievalType, waitMode);
    }
    
    private Object _doRetrieve(ClientOptions options,
            SynchronousNamespacePerspective<String, byte[]> syncNSP, Stopwatch sw, RetrievalType retrievalType,
            WaitMode waitMode) throws OperationException, IOException {
        try {
            SynchronousNamespacePerspective<String, byte[]> ns;
            RetrievalOptions retrievalOptions;
            StoredValue<byte[]> storedValue;
            VersionConstraint vc;

            if (options.maxVersion > 0) {
                // if (options.minVersion >= 0 && options.maxVersion > 0) {
                vc = new VersionConstraint(Long.MIN_VALUE, options.maxVersion, VersionConstraint.Mode.GREATEST);
                // vc = new VersionConstraint(options.minVersion, options.maxVersion, Mode.NEWEST);
            } else {
                vc = VersionConstraint.defaultConstraint;
            }
            retrievalOptions = OptionsHelper.newRetrievalOptions(retrievalType, waitMode, vc);

            Log.warning(retrievalOptions);
            Log.warning("Getting value");
            storedValue = null;
            if (options.action == Action.MultiGet) {
                Set<String> keys;
                Map<String, ? extends StoredValue<byte[]>> storedValues;

                keys = ImmutableSet.copyOf(options.key.split(multiKeyDelimiter));
                storedValues = null;
                sw.reset();
                for (int i = 0; i < options.reps; i++) {
                    // System.out.printf("Calling retrieve %d\n", i);
                    storedValues = syncNSP.retrieve(keys, retrievalOptions);
                    // System.out.printf("Done retrieve %d\n", i);
                }
                sw.stop();
                return storedValues;
            } else {
                sw.reset();
                for (int i = 0; i < options.reps; i++) {
                    // System.out.printf("Calling retrieve %d\n", i);
                    storedValue = syncNSP.retrieve(options.key, retrievalOptions);
                    // System.out.printf("Done retrieve %d\n", i);
                }
                sw.stop();
                return storedValue;
            }
        } catch (RetrievalException re) {
            displayRetrievalExceptionDetails(re);
            throw re;
        }
    }
    
	private void doPut(ClientOptions options, DHTSession session, SynchronousNamespacePerspective<String,byte[]> syncNSP, Stopwatch sw) throws OperationException, IOException {
		PutOptions		putOptions;
		long            version;
		byte[]          value;
		
	    version = options.version;
		putOptions = session.getDefaultPutOptions()
                        .compression(options.compression != null ? options.compression : Compression.NONE)
		                .checksumType(options.checksumType)
		                .version(version);
		Log.warning("Putting value for version:", version);
		value = options.getValue();
        sw.reset();
        try {
    		for (int i = 0; i < options.reps; i++) {
                syncNSP.put(options.key, value);
    			//syncNSP.put(options.key, value, putOptions);
    		}
        } catch (PutException pe) {
            System.out.println(pe.getDetailedFailureMessage());
            throw pe;
        }
        sw.stop();
	}
	
    private void doMultiPut(ClientOptions options, DHTSession session, SynchronousNamespacePerspective<String,byte[]> syncNSP, Stopwatch sw) throws OperationException, IOException {
        PutOptions      putOptions;
        long            version;
        Map<String,byte[]>  map;
        ImmutableMap.Builder<String,byte[]>    builder;
        
        version = options.version;
        builder = ImmutableMap.builder();
        for (int i = 0; i < options.numKeys; i++) {
            builder.put(options.key +"."+ i, options.getValue());
        }
        map = builder.build();
        putOptions = session.getDefaultPutOptions()
                .compression(options.compression != null ? options.compression : Compression.NONE)
                .checksumType(options.checksumType)
                .version(version);
        Log.warning("Putting value for version:", version);
        sw.reset();
        try {
            for (int i = 0; i < options.reps; i++) {
                syncNSP.put(map, putOptions);
            }
        } catch (PutException pe) {
            System.out.println(pe.getDetailedFailureMessage());
            throw pe;
        }
        sw.stop();
    }
    
    private void doSnapshot(ClientOptions options, SynchronousNamespacePerspective<String, byte[]> syncNSP, Stopwatch sw) throws OperationException, IOException {
        long            version;
        
        if (options.version > 0) {
            version = options.version;
        } else {
            version = DHTUtil.currentTimeMillis();
        }
        Log.warning("Creating snapshot");
        sw.reset();
    	// snapshots deprecated for now
        /*
        try {
            for (int i = 0; i < options.reps; i++) {
                syncNSP.snapshot(version);
            }
        } catch (SnapshotException pe) {
            System.out.println(pe.getDetailedFailureMessage());
            throw pe;
        }
        */
        sw.stop();
    }
	
    private void doSyncRequest(ClientOptions options, SynchronousNamespacePerspective<String, byte[]> syncNSP, Stopwatch sw) throws OperationException, IOException {
        long            version;
        
        if (options.version > 0) {
            version = options.version;
        } else {
            version = DHTUtil.currentTimeMillis();
        }
        Log.warning("Creating syncRequest");
        sw.reset();
        // deprecated for now
        /*
        try {
            for (int i = 0; i < options.reps; i++) {
                syncNSP.syncRequest(version);
            }
        } catch (SyncRequestException sre) {
            System.out.println(sre.getDetailedFailureMessage());
            throw sre;
        }
        */
        sw.stop();
    }
    
    private void doCreateNamespace(ClientOptions options, DHTSession session, Stopwatch sw) throws OperationException, IOException {
        long            version;
        NamespaceOptions    nsOptions;
        
        if (options.nsOptions != null) {
            nsOptions = NamespaceOptions.parse(options.nsOptions);
        } else {
            nsOptions = null;
        }
        if (options.version > 0) {
            version = options.version;
        } else {
            version = DHTUtil.currentTimeMillis();
        }
        Log.warning("Creating syncRequest");
        sw.reset();
        for (int i = 0; i < options.reps; i++) {
            session.createNamespace(options.namespace, nsOptions);
        }
        sw.stop();
    }
    
    private void doGetNamespaceOptions(ClientOptions options, DHTSession session, Stopwatch sw) throws OperationException, IOException {
        long                version;
        NamespaceOptions    nsOptions;

        Log.warning("Getting namespace options");
        nsOptions = null;
        sw.reset();
        for (int i = 0; i < options.reps; i++) {
            nsOptions = session.getNamespace(options.namespace).getOptions();
        }
        sw.stop();
        System.out.println(nsOptions);
    }
    
	public void doAction(ClientOptions options) throws Exception {
		DHTSession	session;
		Action		action;
		SynchronousNamespacePerspective<String,byte[]>	syncNSP;
		Stopwatch	sw;
		int			outerReps;
		NamespacePerspectiveOptions<String,byte[]> nspOptions;
		
		// FUTURE - IMPLEMENT VALIDATION FLAG
		//DHTClient.setValidateChecksums(!options.noValidation);
		// note - server may be null
		
        session = dhtClient.openSession(new SessionOptions(SKGridConfiguration.parseFile(options.gridConfig), options.server));
		if (session == null) {
			throw new RuntimeException("null session");
		}
		//System.out.println("nspOptions: "+ nspOptions);
		
		if (options.action != Action.CreateNamespace) {
			Namespace	ns;
			
			ns = session.getNamespace(options.namespace);
			nspOptions = ns.getDefaultNSPOptions(String.class, byte[].class);
			if (options.checksumType != null) {
	            nspOptions = nspOptions.defaultPutOptions(
	                        session.getDefaultPutOptions().checksumType(options.checksumType));
			}
		    syncNSP = ns.openSyncPerspective(nspOptions);
		} else {
		    syncNSP = null;
		}
		//syncNSP = session.openSyncNamespacePerspective(options.namespace, nspOptions);
		if (options.warmup) {
			outerReps = 2;
		} else {
			outerReps = 1;
		}
		for (int i = 0; i < outerReps; i++) {
			sw = new SimpleStopwatch();
			try {
				action = options.action;
				switch (action) {
				case Put:
					doPut(options, session, syncNSP, sw);
					break;
                case MultiPut:
                    doMultiPut(options, session, syncNSP, sw);
                    break;
                case MultiGet:
                    doMultiGet(options, syncNSP, sw);
                    break;
				case Get:
					doGet(options, syncNSP, sw);
					break;
                case GetMeta:
                    doGetMeta(options, syncNSP, sw);
                    break;
                case GetValueAndMeta:
                    doGetValueAndMeta(options, syncNSP, sw);
                    break;
				case WaitFor:
					doWaitFor(options, syncNSP, sw);
					break;
                case Snapshot:
                    doSnapshot(options, syncNSP, sw);
                    break;
                case SyncRequest:
                    doSyncRequest(options, syncNSP, sw);
                    break;
                case CreateNamespace:
                    doCreateNamespace(options, session, sw);
                    break;
                case GetNamespaceOptions:
                    doGetNamespaceOptions(options, session, sw);
                    break;
				default:
					throw new RuntimeException("panic");
				}
			} finally {
				session.close();
			}
			//sw.stop();
			Log.warning("Elapsed:\t"+ sw.getElapsedSeconds());
			if (options.warmup || i < outerReps - 1) {
				ThreadUtil.sleep(warmupDelayMillis);
			}
		}
	}
	
    private void displayRetrievalExceptionDetails(RetrievalException re) {
        System.err.println("Failed keys: ");
        for (Object key : re.getFailedKeys()) {
            System.out.printf("%s\t%s\n", key, re.getFailureCause(key));
        }
    }
		
	/**
	 * @param args
	 */
	public static void main(String[] args) {
    	try {
    		ClientTool		clientTool;
    		ClientOptions	options;
    		CmdLineParser	parser;
    		
            LWTPoolProvider.createDefaultWorkPools(DefaultWorkPoolParameters.defaultParameters().workUnit(clientWorkUnit));
    		options = new ClientOptions();
    		parser = new CmdLineParser(options);
    		try {
    			parser.parseArgument(args);
    		} catch (CmdLineException cle) {
    			System.err.println(cle.getMessage());
    			parser.printUsage(System.err);
    			return;
    		}
    		Log.fine(options);
    		if (options.verbose) {
    		    Log.setLevelAll();
    		}
    		clientTool = new ClientTool();
            System.out.println(options.namespace +":"+ options.key);
    		clientTool.doAction(options);
    		System.exit(0);
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
}

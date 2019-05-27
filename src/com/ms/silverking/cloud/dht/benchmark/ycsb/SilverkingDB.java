package com.ms.silverking.cloud.dht.benchmark.ycsb;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.FailureCause;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.serialization.SerializationRegistry;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.thread.lwt.DefaultWorkPoolParameters;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.UndefinedAction;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

public class SilverkingDB extends DB {
    private DHTSession  _session;
    private SynchronousNamespacePerspective<String, Map> syncNSP;
    private AsynchronousNamespacePerspective<String, Map> asyncNSP;
    
    private static final DHTClient dhtClient;
    private static final SKGridConfiguration  gc;
    private static final DHTSession  session;
        
    private static final boolean    debug = false;
    private static final boolean    verbose = false;
    
    //private static final AtomicLong  version;
    
    private static int  clientWorkUnit = 256;
    //private static int  clientWorkUnit = 1;
    
    private static final String gcPropertyName = "ycsb.GridConfig";
    private static final String gcName;
    
    private static final String nsPropertyName = "ycsb.Namespace";
    private static final String nsName;
    
    // NOTE: YCSB does not appear to support multi-key operations. This is 
    // a major problem in obtaining accurate performance numbers since 
    // Silverking is highly optimized for multi-key operations.

    static {
        LWTPoolProvider.createDefaultWorkPools(DefaultWorkPoolParameters.defaultParameters().workUnit(clientWorkUnit));
        //LWTPoolProvider.defaultConcurrentWorkPool.dumpStatsOnShutdown();
        
        SerializationRegistry   serializationRegistry;
        
        gcName = PropertiesHelper.systemHelper.getString(gcPropertyName, UndefinedAction.ExceptionOnUndefined);
        nsName = PropertiesHelper.systemHelper.getString(nsPropertyName, SilverkingDBConstants.namespace);
        
        System.out.printf("Using namespace: %s\n", nsName);
        
        serializationRegistry = SerializationRegistry.createDefaultRegistry();
        serializationRegistry.addSerDes(Map.class, new RecordSerDes());
        try {
            dhtClient = new DHTClient(serializationRegistry);
            // FUTURE - for general usage we will need to remove these temporary hardcodes used in testing
            gc = SKGridConfiguration.parseFile(gcName);
            session = dhtClient.openSession(gc);
            session.createNamespace(nsName, SilverkingDBConstants.nsOptions);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        //version = new AtomicInteger(((int)(System.currentTimeMillis() * 10000) >>> 2) & 0x3fffffff);
        //version = new AtomicLong(System.nanoTime());
        //System.out.printf("Using version: %d\t%x\n", version.get(), version.get());
        
    }
    
    public void init() throws DBException {
        try {
        	ThreadUtil.sleep(2000);
            if (session == null) {
                throw new RuntimeException("null session");
            }
            _session = dhtClient.openSession(gc); // session per thread
            //_session = session; // use the global session
            syncNSP = _session.openSyncNamespacePerspective(nsName, String.class, Map.class);
            asyncNSP = _session.openAsyncNamespacePerspective(nsName, String.class, Map.class);
            System.out.printf("Init %s %s\n", Thread.currentThread().getName(), _session.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        //LWTThreadUtil.setLWTThread();
    }
    
    /*
    private static ConcurrentMap<String,AtomicLong>   versionMap = new ConcurrentHashMap<>();
    
    private static long versionForKey(String key) {
        AtomicLong  version;
        
        version = versionMap.get(key);
        if (version == null) {
            versionMap.putIfAbsent(key, new AtomicLong(1));
            version = versionMap.get(key);
        }
        return version.getAndIncrement();
    }
    */
    
    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            if (verbose) {
                System.out.println("Inserting:\t"+ key);
            }
            //for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            //    System.out.println(entry.getKey() +"\t"+ entry.getValue().toString());
            //}
            syncNSP.put(key, values);
            //syncNSP.put(key, values, putOptions);
            /*
            {
                AsyncPut    asyncPut;
                
                //System.out.println("Waiting for: "+ key);
                asyncPut = asyncNSP.put(key, values);
                try {
                    asyncPut.waitForCompletion(30, TimeUnit.SECONDS);
                    if (asyncPut.getState() == OperationState.INCOMPLETE) {
                        System.out.println("Incomplete insertion: "+ key);
                        System.out.flush();
                        System.exit(-1);
                    }
                } catch (OperationException e) {
                    PutException    pe;
                    
                    pe = (PutException)e;
                    if (pe.getFailureCause(key) == FailureCause.INVALID_VERSION) {
                        System.out.println("Ignoring INVALID_VERSION");
                        System.err.println("Ignoring INVALID_VERSION");
                        return 1;
                    } else if (pe.getFailureCause(key) == FailureCause.MUTATION) {
                            System.out.println("Ignoring MUTATION");
                            System.err.println("Ignoring MUTATION");
                            return 1;
                    } else {
                        throw new RuntimeException(pe);
                    }
                }
            }
            */
            //System.out.println("Done waiting for: "+ key);
            return 0;
        } catch (PutException pe) {
        	if (pe.getFailureCause(key) == FailureCause.INVALID_VERSION) {
        		// Storage is moot, as a more recent version has overridden this version
        		// Could consider running this test with revision support, but that would be significantly
        		// different than most other stores as they generally are mutable with only one version supported.
        		return 2;
        	} else {
        		pe.printStackTrace();
	            System.out.println("Key failed: "+ key +" "+ pe +" "+ pe.getFailureCause(key));
	            return 1;
        	}
        }
    }
    
    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> values) {
        Map<String, ByteIterator>   _values;
        /*
        AsyncSingleValueRetrieval<String,Map>  asvRetrieval;
        
        if (verbose) {
            System.out.println("read:\t"+ key);
        }
        try {
            asvRetrieval = asyncNSP.get(key);
            try {
                asvRetrieval.waitForCompletion(30, TimeUnit.SECONDS);
                if (asvRetrieval.getState() == OperationState.INCOMPLETE) {
                    System.out.println("Incomplete read: "+ key);
                    System.out.flush();
                    System.exit(-1);
                }
                _values = asvRetrieval.getValue();
                if (_values != null) {
                    values.putAll(_values);
                    return 0;
                } else {
                    // Verify that YCSB generates only keys generated during load
                    if (verbose) {
                        System.out.println("Missing:\t"+ key);
                    }
                    return 1;
                }
            } catch (OperationException oe) {
                throw new RuntimeException(oe);
            }
        } catch (RetrievalException re) {
            return 1;
        }
        */
        /*
        Map<String, ByteIterator>   _values;
        */
        //System.out.println("read: "+ key);
        try {
            _values = syncNSP.get(key);
            if (_values != null) {
                values.putAll(_values);
                return 0;
            } else {
                System.out.println("Missing:\t"+ key);
                return 1;
            }
        } catch (RetrievalException re) {
        	System.out.println(re);
        	re.printStackTrace();
            return 1;
        }
        /**/
    }

    @Override
    public int delete(String table, String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int scan(String table, String startKey, int recourdCount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        return insert(table, key, values);
    }
}

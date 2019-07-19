package com.ms.silverking.cloud.dht.benchmark.ycsb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.ms.silverking.cloud.dht.client.AsyncPut;
import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.FailureCause;
import com.ms.silverking.cloud.dht.client.OperationException;
import com.ms.silverking.cloud.dht.client.OperationState;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.serialization.SerializationRegistry;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.collection.LightLinkedBlockingQueue;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.thread.lwt.DefaultWorkPoolParameters;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.UndefinedAction;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

public class GroupingSilverkingDB extends DB {
    private static final DHTClient dhtClient;
    private static final DHTSession  session;
    private SynchronousNamespacePerspective<String, Map> syncNSP;
    private AsynchronousNamespacePerspective<String, Map> asyncNSP;
    
    // NOTE: YCSB does not appear to support multi-key operations. This is
    // a major problem in obtaining accurate performance numbers since 
    // Silverking is highly optimized for multi-key operations.
    
    private static final int    numWorkers = 10;
    
    private static final boolean    debug = false;
    
    private static final AtomicLong  version;
    
    private static int  clientWorkUnit = 1000;
    
    private static final String gcPropertyName = "ycsb.GridConfig";
    private static final String gcName;

    static {
        LWTPoolProvider.createDefaultWorkPools(DefaultWorkPoolParameters.defaultParameters().workUnit(clientWorkUnit));

        gcName = PropertiesHelper.systemHelper.getString(gcPropertyName, UndefinedAction.ExceptionOnUndefined);
        
        SerializationRegistry   serializationRegistry;
        
        serializationRegistry = SerializationRegistry.createDefaultRegistry();
        serializationRegistry.addSerDes(Map.class, new RecordSerDes());
        System.err.println("GroupingSilverkingDB must be updated before it is used"); // FUTURE
        try {
            dhtClient = new DHTClient(serializationRegistry);
            session = dhtClient.openSession(SKGridConfiguration.parseFile(gcName));
            //session.createNamespace(SilverkingDBConstants.defaultNamespace, SilverkingDBConstants.nsOptions(session));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        readOps = new LightLinkedBlockingQueue<>();  
        for (int i = 0; i < numWorkers; i++) {
            //ThreadUtil.newDaemonThread(new Worker(session.openSyncNamespacePerspective(SilverkingDBConstants.defaultNamespace, SilverkingDBConstants.nspOptions)), "Worker"+ i).start();
        }        
        //version = new AtomicInteger(((int)(System.currentTimeMillis() * 10000) >>> 2) & 0x3fffffff);
        version = new AtomicLong(System.nanoTime());
    }
    
    public void init() throws DBException {
        try {
            if (session == null) {
                throw new RuntimeException("null session");
            }
            //syncNSP = session.openSyncNamespacePerspective(SilverkingDBConstants.defaultNamespace, SilverkingDBConstants.nspOptions);
            //asyncNSP = session.openAsyncNamespacePerspective(SilverkingDBConstants.defaultNamespace, SilverkingDBConstants.nspOptions);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    
    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        try {            
            //for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            //    System.out.println(entry.getKey() +"\t"+ entry.getValue().toString());
            //}
            if (System.currentTimeMillis() == 0) {
                syncNSP.put(key, values);
            }
            /**/
            {
                AsyncPut    asyncPut;
                
                //System.out.println("Waiting for: "+ key);
                asyncPut = asyncNSP.put(key, values);
                try {
                    asyncPut.waitForCompletion(30, TimeUnit.SECONDS);
                    if (asyncPut.getState() == OperationState.INCOMPLETE) {
                        System.out.println("Incomplete: "+ key);
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
                    } else {
                        throw new RuntimeException(pe);
                    }
                }
            }
            /**/
            //System.out.println("Done waiting for: "+ key);
            return 0;
        } catch (PutException pe) {
            System.out.println("Key failed: "+ key);
            return 1;
        }
    }
    
    private class ReadOp {
        final String        table;
        final String        key;
        final Set<String>   fields;
        final HashMap<String, ByteIterator> values;
        int                 returnCode;
        private boolean     complete;
        // FUTURE - CONSIDER MAKING A METHOD FOR THE THREADS TO WAIT FOR THE OPS TO COMPLETE
        
        public ReadOp(String table, String key, Set<String> fields, HashMap<String, ByteIterator> values) {
            this.table = table;
            this.key = key;
            this.fields = fields;
            this.values = values;
        }
        
        public void waitForCompletion() {
            synchronized (this) {
                while (!complete) {
                    try {
                        this.wait();
                    } catch (InterruptedException ie) {
                    }
                }
            }
        }
        
        public boolean waitForCompletion(TimeUnit timeUnit, int timeVal) {
            synchronized (this) {
                long    deadlineMillis;
                
                deadlineMillis = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(timeVal, timeUnit);
                while (!complete) {
                    try {
                        long    curTimeMillis;
                        
                        curTimeMillis = System.currentTimeMillis();
                        if (curTimeMillis < deadlineMillis) {
                            this.wait((int)(deadlineMillis - curTimeMillis));
                        } else {
                            return complete;
                        }
                    } catch (InterruptedException ie) {
                    }
                }
                return complete;
            }
        }
        
        public void setComplete(int result) {
            synchronized (this) {
                if (debug) {
                    System.out.println("Complete: "+ key +"\t"+ result +"\t"+ this);
                }
                if (!complete) {
                    returnCode = result;
                    complete = true;
                    this.notifyAll();
                } else {
                    System.err.println("Unexpected double ReadOp completion.");
                }
            }
        }
    }
    
    
    /*
    private void read(List<ReadOp> readOps) {
        HashSet<String> keys;
        
        keys = new HashSet<>();
        for (ReadOp readOp : readOps) {
            keys.add(readOp.key);
        }
        
        //System.out.println("read: "+ key);
        try {
            Map<String,Map>   _valueMap;
            
            _valueMap = syncNSP.get(keys);
            for (ReadOp readOp : readOps) {
                Map<String, ByteIterator>   _values;
                
                _values = _valueMap.get(readOp.key);
                if (_valueMap != null) {
                    readOp.values.putAll(_values);
                    readOp.returnCode = 0;
                } else {
                    System.out.println("Missing:\t"+ readOp.key);
                    readOp.returnCode = 1;
                }
            }
        } catch (RetrievalException re) {
            for (ReadOp readOp : readOps) {
                readOp.returnCode = 1;
            }
        }
    }
    */
    
    private static final LightLinkedBlockingQueue<ReadOp>  readOps; 

    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> values) {
        try {
            ReadOp  readOp;
            boolean complete;
            
            readOp = new ReadOp(table, key, fields, values);
            readOps.put(readOp);
            if (debug) {
                System.out.println("waiting for: "+ key +"\t"+ readOp);
            }
            complete = readOp.waitForCompletion(TimeUnit.SECONDS, 5);
            if (debug) {
                System.out.println("back from wait: "+ key +" "+ complete +"\t"+ readOp);
            }
            return readOp.returnCode;
        } catch (InterruptedException ie) {
            throw new RuntimeException("Unsupported interruption");
        }
    }
    
    private static class Worker implements Runnable {
        private final ReadOp[]  groupOps;
        private final SynchronousNamespacePerspective<String, Map> syncNSP;
        
        private static final int    maxGroupSize = 1024;
        
        private Worker(SynchronousNamespacePerspective<String, Map> syncNSP) {
            this.syncNSP = syncNSP;
            groupOps = new ReadOp[maxGroupSize];
        }
        
        public void run() {
            while (true) {
                try {
                    int groupSize;
                    
                    groupSize = readOps.takeMultiple(groupOps);
                    read(groupOps, groupSize);
                } catch (InterruptedException ie) {
                }
            }
        }

        private void displayList(List list) {
            System.out.println(list.size());
            for (int i = 0; i < list.size(); i++) {
                System.out.printf("%d\t%s\n", i, list.get(i));
            }
        }
        
        private void read(ReadOp[] ops, int numOps) {
            Map<String,Map> values;
            Map<String,Object> keyToOp;
            
            if (debug) {
                System.out.println("\n\n***************\tread "+ numOps);
            }
            keyToOp = new HashMap<>(numOps);
            for (int i = 0; i < numOps; i++) {
                Object  existing;
                ReadOp  op;
                
                op = ops[i];
                existing = keyToOp.put(op.key, op);
                if (existing != null) {
                    if (debug) {
                        System.out.println("Multiple waiter:\t"+ op);
                    }
                    if (existing instanceof ReadOp) {
                        List<ReadOp>    opList;
                        
                        if (debug) {
                            System.out.println("multiple found for:\t"+ op.key);
                        }
                        opList = new ArrayList<>();
                        opList.add((ReadOp)existing);
                        opList.add(op);
                        keyToOp.put(op.key, opList);
                        if (debug) {
                            displayList(opList);
                        }
                    } else {
                        List<ReadOp>    opList;
                        
                        opList = (List<ReadOp>)existing;
                        opList.add(op);
                        if (debug) {
                            displayList(opList);
                        }
                        keyToOp.put(op.key, existing);
                    }
                } else {
                    if (debug) {
                        System.out.println("Single waiter:\t"+ op.key +"\t"+ op);
                    }
                }
           }
            try {
                if (debug) {
                    System.out.println("calling get");
                }
                values = syncNSP.get(keyToOp.keySet());
                if (debug) {
                    System.out.println("get complete");
                }
                if (values.entrySet().size() != keyToOp.keySet().size()) {
                    throw new RuntimeException("size mismatch");
                }
                for (Map.Entry<String,Map> result : values.entrySet()) {
                    String  key;
                    Object  opOrList;
                    ReadOp  op;
                    
                    key = result.getKey();
                    opOrList = keyToOp.get(key);
                    if (opOrList instanceof ReadOp) {
                        op = (ReadOp)opOrList;
                        if (debug) {
                            System.out.println("singleResult: "+ key);
                        }
                        handleResult(op, result);
                    } else {
                        List<ReadOp>    opList;
                        
                        opList = (List<ReadOp>)opOrList;
                        if (debug) {
                            System.out.println("multipleResults: "+ key);
                        }
                        for (ReadOp _op : opList) {
                            handleResult(_op, result);
                        }
                    }
                }
            } catch (RetrievalException re) {
                re.printStackTrace();
                for (Object opOrList : keyToOp.values()) {
                    ReadOp  op;
                    
                    if (opOrList instanceof ReadOp) {
                        op = (ReadOp)opOrList;
                        op.setComplete(1);
                    } else {
                        List<ReadOp>    opList;
                        
                        opList = (List<ReadOp>)opOrList;
                        for (ReadOp _op : opList) {
                            _op.setComplete(1);
                        }
                    }
                }
            }
            if (debug) {
                System.out.println("out read "+ numOps);
            }
        }
        
        private void handleResult(ReadOp op, Map.Entry<String,Map> result) {
            HashMap<String,ByteIterator>    keyValues;
            
            if (debug) {
                System.out.println("handleResult: "+ op);
            }
            keyValues = (HashMap<String,ByteIterator>)result.getValue();
            if (keyValues != null) {
                for (Map.Entry<String,ByteIterator> fieldEntry : keyValues.entrySet()) {
                    op.values.put(fieldEntry.getKey(), fieldEntry.getValue());
                }
                op.setComplete(0);
            } else {
                op.setComplete(1);
            }
        }
        
        private int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> values) {
            Map<String, ByteIterator>   _values;
            
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
                return 1;
            }
        }
        
        /*
        public void run() {
            while (true) {
                ReadOp  readOp;
                int     result;
                
                try {
                    readOp = readOps.take();
                    result = read(readOp.table, readOp.key, readOp.fields, readOp.values);
                    readOp.setComplete(result);
                } catch (InterruptedException ie) {
                }
            }
        }
        
        public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> values) {
            Map<String, ByteIterator>   _values;
            
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
                return 1;
            }
        }
        */
    }

    
    
    /*
    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> values) {
        Map<String, ByteIterator>   _values;
        
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
            return 1;
        }
    }
    */

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

package com.ms.silverking.cloud.dht.benchmark.ycsb;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.NamespacePerspectiveOptions;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.serialization.SerializationRegistry;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

public class SilverkingFieldSpecificDB extends DB {
    private static final DHTClient dhtClient;
    private SynchronousNamespacePerspective<String, ByteIterator> syncNSP;
    private Set<String> allFields;
    
    // FUTURE - add a map of namespaces; use a new namespace for each table?

    // NOTE: YCSB does not appear to support multi-key operations. This is
    // a major problem in obtaining accurate performance numbers since 
    // Silverking is highly optimized for multi-key operations.
    
    private static final String fieldPrefix = "field";
    private static final int    numFields = 10;

    static {
        LWTPoolProvider.createDefaultWorkPools();
        
        SerializationRegistry   serializationRegistry;
        
        serializationRegistry = SerializationRegistry.createDefaultRegistry();
        serializationRegistry.addSerDes(ByteIterator.class, new ByteIteratorSerDes());
        
        try {
            dhtClient = new DHTClient(serializationRegistry);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public SilverkingFieldSpecificDB() {
    }
    
    public void init() throws DBException {
        try {
            DHTSession  session;
            //NamespacePerspectiveOptions<String,ByteIterator> nspOptions;
            ImmutableSet.Builder<String> builder;
            
            //session = dhtClient.openSession(new ClientDHTConfiguration("instance name", "ip:port"));
            System.err.println("Update SilverkingFieldSpecificDB before use"); // FUTURE - fix if this test is needed 
            session = null;
            if (session == null) {
                throw new RuntimeException("null session");
            }
            /*
            nspOptions = new NamespacePerspectiveOptions<>(String.class, ByteIterator.class);
            nspOptions = nspOptions.defaultPutOptions(nspOptions.getDefaultPutOptions().compression(Compression.NONE).checksumType(ChecksumType.NONE));
            System.out.println(nspOptions);
            */
            //syncNSP = session.openSyncNamespacePerspective(SilverkingDBConstants.namespace, SilverkingDBConstants.nspOptions);
            builder = new ImmutableSet.Builder<>();
            for (int i = 0; i < numFields; i++) {
                builder.add(fieldPrefix + i);
            }
            allFields = builder.build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    private String createFieldKey(String key, String field) {
        StringBuilder   sb;
        
        sb = new StringBuilder();
        sb.append(key);
        if (field != null) {
            sb.append(field);
        }
        return sb.toString();
    }
    
    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        Map<String, ByteIterator>   _values;
        
        _values = new HashMap<>(values.size());
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            //System.out.println("insert: "+ createFieldKey(key, entry.getKey()));
            _values.put(createFieldKey(key, entry.getKey()), entry.getValue());
        }
        try {
            syncNSP.put(_values);
            return 0;
        } catch (PutException pe) {
            return 1;
        }
        /*
        Map.Entry<String, ByteIterator>   entry;
        
        entry = values.entrySet().iterator().next();
        try {
            syncNSP.put(key, entry.getValue().toArray());
        } catch (PutException pe) {
            throw new RuntimeException(pe);
        }
        return 0;
        */
    }

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> values) {
        //String[]  _fields;
        String[]  _fieldKeys;
        Set<String> fieldKeySet;
        Map<String, ByteIterator>   _values;
        int i;
        
        //System.out.println("values.size(): "+ values.size());
        //System.out.println("fields.size(): "+ fields.size());
        //_fields = new String[fields.size()];
        if (fields == null) {
            fields = allFields;
        }
        if (fields != null) {
            _fieldKeys = new String[fields.size()];
            i = 0;
            for (String field : fields) {
                //_fields[i] = field;
                //System.out.println("retrieve: "+ createFieldKey(key, field));
                _fieldKeys[i] = createFieldKey(key, field);
                i++;
            }
            fieldKeySet = ImmutableSet.copyOf(_fieldKeys);
            try {
                _values = syncNSP.get(fieldKeySet);
                values.putAll(_values);
                return 0;
            } catch (RetrievalException re) {
                return 1;
            }
        } else {
            try {
                values.put(key, syncNSP.get(key));
                return 0;
            } catch (RetrievalException re) {
                return 1;
            }
        }
        /*
        try {
            String  field;
            byte[]  value;
            
            //field = fields.iterator().next();
            try {
                value = syncNSP.get(key);
                values.put(key, new ByteArrayByteIterator(value));
            } catch (RuntimeException re) {
                System.out.println(re);
            }
        } catch (RetrievalException re) {
            throw new RuntimeException(re);
        }
        return 0;
        */
    }

    @Override
    public int delete(String table, String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int scan(String arg0, String arg1, int arg2, Set<String> arg3, Vector<HashMap<String, ByteIterator>> arg4) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        return insert(table, key, values);
    }

}

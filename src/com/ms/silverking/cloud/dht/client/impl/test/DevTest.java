package com.ms.silverking.cloud.dht.client.impl.test;

import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.ms.silverking.cloud.dht.GetOptions;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.client.AsyncPut;
import com.ms.silverking.cloud.dht.client.AsyncRetrieval;
import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.StoredValue;
import com.ms.silverking.cloud.dht.common.OptionsHelper;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.net.HostAndPort;
import com.ms.silverking.net.NetUtil;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

public class DevTest {
    private final int valueSize;
    private final HostAndPort[] zkLocs;
    private final String    dhtName;
    
    private static final boolean    displayValues = false;
    
    private enum Test {Put, Get};
    
    public DevTest(HostAndPort[] zkLocs, String dhtName, int valueSize) {
        this.zkLocs = zkLocs;
        this.dhtName = dhtName;
        this.valueSize = valueSize;
        System.out.println("valueSize: "+ valueSize);
    }
    
    private void displayTimes(Stopwatch sw, int reps, int numKeys, int bytesPerKey) {
        int     totalOps;
        double  usPerOp;
        
        System.out.println(sw);
        totalOps = reps * numKeys;
        usPerOp = 1.0e6 * sw.getElapsedSeconds() / (double)totalOps;
        System.out.printf("usPerOp %f\n", usPerOp);
        System.out.printf("Mbps %f\n", NetUtil.calcMbps(reps * numKeys * bytesPerKey, sw.getElapsedSeconds()));
    }
    
    public void test(int numKeys, int reps, String namespace, Compression compression, ChecksumType checksumType, EnumSet<Test> tests) throws Exception {
        DHTClient           client;
        ClientDHTConfiguration    dhtConfig;
        DHTSession          session;
        AsynchronousNamespacePerspective<String,String>    asyncNSP;
        AsyncRetrieval<String,String>    asyncRetrieval;
        AsyncPut<String>    asyncPut;
        Stopwatch           sw;
        PutOptions          putOptions;
        
        asyncPut = null;
        client = new DHTClient();
        dhtConfig = new ClientDHTConfiguration(dhtName, new ZooKeeperConfig(zkLocs));
        session = client.openSession(dhtConfig);
        putOptions = session.getDefaultNamespaceOptions().getDefaultPutOptions().compression(compression).checksumType(checksumType);
        asyncNSP = session.openAsyncNamespacePerspective(namespace, String.class, String.class);
        sw = new SimpleStopwatch();
        if (tests.contains(Test.Put)) {
            System.out.println("\n\n\t\tPUT");
            for (int i = 0; i < reps; i++) {
                //asyncPut = asyncNSP.put("Hello"+ i, "world!");
                asyncPut = asyncNSP.put(createMap(i, numKeys), putOptions);
                asyncPut.waitForCompletion();
            }
            sw.stop();
            displayTimes(sw, reps, numKeys, valueSize);
        }
        
        if (tests.contains(Test.Get)) {
            System.out.println("\n\n\t\tGET");
            GetOptions  getOptions;
            
            sw.reset();
            getOptions = OptionsHelper.newGetOptions(RetrievalType.VALUE_AND_META_DATA,
                    session.getDefaultNamespaceOptions().getDefaultGetOptions().getVersionConstraint());
            for (int i = 0; i < reps; i++) {
            	Set<String>	keys;
                Map<String, ? extends StoredValue<String>> values;
                
                keys = createSet(i, numKeys);                
                asyncRetrieval = asyncNSP.get(keys, getOptions);
                asyncRetrieval.waitForCompletion();
                if (displayValues) {
                	System.out.printf("keys: %s\n", CollectionUtil.toString(keys));
                    values = asyncRetrieval.getStoredValues();
                	System.out.printf("values: %s\n", CollectionUtil.toString(values.entrySet()));                    
                    for (Entry<String, ? extends StoredValue<String>> entry : values.entrySet()) {
                        System.out.println(entry.getKey() +" -> "+ entry.getValue().getValue() +"\t"+ entry.getValue().getMetaData().toString(true));
                    }
                }
            }
            sw.stop();
            displayTimes(sw, reps, numKeys, valueSize);
        }
    }
    
    private Map<String,String> createMap(int id, int numKeys) {
        Map<String,String>  map;
        
        map = new HashMap<>();
        for (int i = 0; i < numKeys; i++) {
            byte[]  value;
            byte[]  content;
            
            content = ("value"+ i +" "+ new Date().toString()).getBytes();
            value = new byte[valueSize];
            System.arraycopy(content, 0, value, 0, Math.min(content.length, valueSize));
            map.put("key_"+id +":"+ i, new String(value));
        }
        return map;
    }

    private Set<String> createSet(int id, int numKeys) {
        Set<String>  set;
        
        set = new HashSet<>();
        for (int i = 0; i < numKeys; i++) {
            set.add("key_"+id +":"+ i);
        }
        return set;
    }
    
    private static EnumSet<Test> parseTests(String def) {
        String[]        testNames;
        EnumSet<Test>   tests;
        
        testNames = def.split(":");
        tests = EnumSet.noneOf(Test.class);
        for (String testName : testNames) {
            tests.add(Test.valueOf(testName));
        }
        return tests;
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            if (args.length != 9) {
                System.out.println("args: <zkLocs> <dhtName> <valueSize> <numKeys> <reps> <namespace> <compression> <checksum> <tests>");
                return;
            } else {
                HostAndPort[]   zkLocs;
                String dhtName;
                int valueSize;
                int numKeys;
                int reps;
                String      namespace;
                Compression compression;
                ChecksumType    checksumType;
                EnumSet<Test>   tests;
                
                LWTPoolProvider.createDefaultWorkPools();
                zkLocs = HostAndPort.parseMultiple(args[0]);
                dhtName = args[1];
                valueSize = Integer.parseInt(args[2]);
                numKeys = Integer.parseInt(args[3]);
                reps = Integer.parseInt(args[4]);
                namespace = args[5];
                compression = Compression.valueOf(args[6]);
                checksumType = ChecksumType.valueOf(args[7]);
                tests = parseTests(args[8]);
                //Log.setLevelAll();
                new DevTest(zkLocs, dhtName, valueSize).test(numKeys, reps, namespace, compression, checksumType, tests);
                //ThreadUtil.sleep(60000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

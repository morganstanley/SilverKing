package com.ms.silverking.cloud.dht.client.apps.test;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.StorageType;
import com.ms.silverking.cloud.dht.client.AsyncValueRetrieval;
import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.StoredValue;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.collection.HashedListMap;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.NetUtil;
import com.ms.silverking.numeric.StatSeries;
import com.ms.silverking.thread.lwt.DefaultWorkPoolParameters;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;
import com.ms.silverking.util.jvm.JVMUtil;

public class BulkThroughputAnalysis {
    private final PrintStream       out;
    private final PrintStream       err;
    private SynchronousNamespacePerspective<String,String>  replicasNSP;
    private final byte[][]          values;
    private int stackTraces;
    private final Stopwatch runSW;
    private final SKGridConfiguration gridConfig;
    private final DHTClient dhtClient;
    private final String      nsName;

    private static final int    stackTraceLimit = 3;
    
    private final Map<String,Double>    keyTimes;
    private final HashedListMap<String,Double>  replicaTimes;
    
    private static final String nsBase = "_BulkThroughput.";
    
    public BulkThroughputAnalysis(SKGridConfiguration gridConfig, PrintStream out, PrintStream err, 
                          BulkThroughputOptions options) throws ClientException, IOException {
        this.gridConfig = gridConfig;
        dhtClient = new DHTClient();
        
        nsName = nsBase + options.id;
        this.out = out;
        this.err = err;
        values = createValues(options.batchSize, options.valueSize);
        
        replicasNSP = null;
        //replicasNSP = session.openSyncNamespacePerspective(Namespace.replicasName, 
        //                new NamespacePerspectiveOptions<String,String>(String.class,String.class));

        keyTimes = new HashMap<>();
        replicaTimes = new HashedListMap<String,Double>();
        runSW = new SimpleStopwatch();
    }

    public void runParallelTest(BulkThroughputTest test, TestParameters p, int externalReps) throws ClientException {
        new TestRunner(test, p, externalReps);
    }

    public void runParallelTests_B(BulkThroughputTest test, TestParameters p, int externalReps) throws PutException, RetrievalException {
        List<Double>    throughputList;
        List<Double>    allBatchTimes;

        allBatchTimes = new ArrayList<>(externalReps * ((p.maxKey - p.minKey) / p.batchSize + 1));
        
        throughputList = new ArrayList<>(externalReps);
        for (int j = 0; j < externalReps; j++) {
            Stopwatch   sw;
            double      _Mbps;
            int         valueSize;
            List<Double>    batchTimes;
            
            sw = new SimpleStopwatch();
            batchTimes = null; // temporary
            
            switch (test) {
            case Write: valueSize = values[0].length; break;
            default: valueSize = -1; 
            }
            
            allBatchTimes.addAll(batchTimes);
            
            long    bytes;
    
            out.printf("valueSize %d\n\n", valueSize);
            
            StatSeries  batchStats;
            
            batchStats = new StatSeries(batchTimes);
            
            bytes = calcBytes(p, valueSize);
            _Mbps = NetUtil.calcMbps(bytes, sw);
            out.printf("Elapsed           %s\n", sw);
            out.printf("Bytes             %d\n", bytes);
            out.printf("Throughput (Mbps) %f\n", _Mbps);
            out.printf("Sum               %f\n", batchStats.sum());
            out.printf("Max               %f\n", batchStats.max());
            out.printf("50%%               %f\n", batchStats.percentile(50));
            out.printf("95%%               %f\n", batchStats.percentile(90));
            out.printf("90%%               %f\n", batchStats.percentile(95));
            out.printf("99%%               %f\n", batchStats.percentile(99));
            throughputList.add(_Mbps);
        }
        
        StatSeries  allBatchStats;
        
        allBatchStats = new StatSeries(allBatchTimes);
        out.printf("\n\nSum               %f\n\n", allBatchStats.sum());
        out.printf("Max               %f\n\n", allBatchStats.max());
        for (int i = 0; i <= 95; i+= 5) {
            out.printf("%3d%%\t%f\n", i, allBatchStats.percentile(i));
        }
        out.printf("%3d%%\t%f\n", 99, allBatchStats.percentile(99));
        
        out.println(StatSeries.summaryHeaderStringLow());
        out.println(new StatSeries(throughputList).toSummaryStringLow());
    
        out.println("\n\n");
        //processReplicaTimeStats();
    }
    
    class TestRunner implements Runnable {
        private final BulkThroughputTest    test;
        private final TestParameters        p;
        private final int                   externalReps;
        private AsynchronousNamespacePerspective<String,byte[]> asyncNSP;
        private SynchronousNamespacePerspective<String,byte[]>  syncNSP;
        
        TestRunner(BulkThroughputTest test, TestParameters p, int externalReps) throws ClientException {
            DHTSession  session;
            Namespace   ns;
            NamespaceOptions    nsOptions;
            
            this.test = test;
            this.p = p;
            this.externalReps = externalReps;
            session = dhtClient.openSession(gridConfig);
            nsOptions = session.getDefaultNamespaceOptions()
                            .storageType(StorageType.RAM)
                            .consistencyProtocol(ConsistencyProtocol.LOOSE)
                            .defaultPutOptions(session.getDefaultPutOptions()
                                                .compression(Compression.NONE)
                                                .checksumType(ChecksumType.NONE));
            ns = session.createNamespace(nsName, nsOptions);
            syncNSP = ns.openSyncPerspective(String.class, byte[].class);
            asyncNSP = ns.openAsyncPerspective(String.class, byte[].class);
            new Thread(this).start();
        }
        
        public void run() {
            try {
                runTest(test, p, externalReps);
            } catch (PutException pe) {
                pe.printStackTrace();
                System.err.println(pe.getDetailedFailureMessage());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        public void write(TestParameters p) throws PutException {
            int k;
            
            k = p.minKey;
            while (k <= p.maxKey) {
                int batchSize;
                
                batchSize = Math.min(p.batchSize, p.maxKey - k + 1);
                syncNSP.put(createMap(k, batchSize));
                k += batchSize;
            }
        }
        
        public int read(TestParameters p, List<Double> batchTimes) throws RetrievalException {
            int k;
            int readSize;
            Stopwatch   batchSW;
            
            batchSW = new SimpleStopwatch();
            readSize = -1;
            k = p.minKey;
            while (k <= p.maxKey) {
                int batchSize;
                Map<String,byte[]>  values;
                
                batchSize = Math.min(p.batchSize, p.maxKey - k + 1);
                batchSW.reset();
                values = syncNSP.get(createSet(k, batchSize));
                batchSW.stop();
                batchTimes.add(batchSW.getElapsedSeconds());
                try {
                    //System.out.printf("key: %d %s\n", k, key(k));
                    readSize = values.get(key(k)).length;
                } catch (NullPointerException npe) {
                    System.out.printf("Couldn't find key: %d %s\n", k, key(k));
                    throw npe;
                }
                k += batchSize;
            }
            return readSize;
        }
        
        public int readAsync(TestParameters p, List<Double> batchTimes) throws RetrievalException {
            int         k;
            int         readSize;
            Stopwatch   batchSW;
            
            batchSW = new SimpleStopwatch();
            readSize = -1;
            k = p.minKey;
            while (k <= p.maxKey) {
                int batchSize;
                AsyncValueRetrieval<String,byte[]>  asyncRetrieval;
                double  lastKeyElapsed;
                int keysComplete;
                boolean done;

                stackTraces = 0;
                keysComplete = 0;
                lastKeyElapsed = 0.0;
                batchSize = Math.min(p.batchSize, p.maxKey - k + 1);
                batchSW.reset();
                asyncRetrieval = asyncNSP.get(createSet(k, batchSize));
                done = false;
                while (!done) {
                    Map<String, ? extends StoredValue<byte[]>>  latestResults;
                    double  curElapsed;
                    
                    latestResults = asyncRetrieval.getLatestStoredValues();
                    curElapsed = batchSW.getSplitSeconds();
                    storeKeyTimes(latestResults, curElapsed);
                    
                    if (!latestResults.keySet().isEmpty()) {
                        String  _k;
                        
                        _k = latestResults.keySet().iterator().next();
                        readSize = latestResults.get(_k).getValue().length;
                        lastKeyElapsed = curElapsed;
                    }
                    
                    //done = asyncRetrieval.getState() != OperationState.INCOMPLETE;
                    done = keysComplete == batchSize;
                    keysComplete += latestResults.keySet().size();
                    //out.printf("keysComplete %d\tbatchSize %d\tcurElapsed %f\tbatchElapsed %f\tdone %s\n", 
                    //        keysComplete, batchSize, curElapsed, batchSW.getSplitSeconds(), done);
                    //if (keysComplete == batchSize) {
                        if (curElapsed > 0.020) {
                            out.println(new Date() +" "+ System.currentTimeMillis() +" "+ CollectionUtil.toString(latestResults.keySet()));
                            if (runSW.getSplitSeconds() > 5.0) {
                                if (stackTraces < stackTraceLimit) {
                                    stackTraces++;
                                    JVMUtil.dumpStackTraces();
                                }
                            }
                        }
                    //}
                    
                    //if (!done) {
                    //    ThreadUtil.sleep(1);
                    //}
                }
                batchSW.stop();
                batchTimes.add(batchSW.getElapsedSeconds());            
                k += batchSize;
                
                if (batchSW.getElapsedSeconds() - lastKeyElapsed > 0.01) {
                    System.out.printf("\t\tCompletion lag: %f\t%f\t%f\n", 
                            batchSW.getElapsedSeconds(), lastKeyElapsed, batchSW.getElapsedSeconds() - lastKeyElapsed);
                }
            }
            return readSize;
        }        
        
    public void runTest3(BulkThroughputTest test, TestParameters p, int externalReps) throws PutException, RetrievalException {
        List<Double>    throughputList;
        List<Double>    allBatchTimes;

        allBatchTimes = new ArrayList<>(externalReps * ((p.maxKey - p.minKey) / p.batchSize + 1));
        
        throughputList = new ArrayList<>(externalReps);
        for (int j = 0; j < externalReps; j++) {
            Stopwatch   sw;
            double      _Mbps;
            int         valueSize;
            List<Double>    batchTimes;
            
            switch (test) {
            case Write: valueSize = values[0].length; break;
            default: valueSize = -1; 
            }
            
            batchTimes = new ArrayList<>((p.maxKey - p.minKey) / p.batchSize + 1);
            
            sw = new SimpleStopwatch();
            for (int i = 0; i < p.repetitions; i++) {
                switch (test) {
                case Write: write(p); break;
                case Read: valueSize = read(p, batchTimes); break;
                case ReadAsync: valueSize = readAsync(p, batchTimes); break;
                default: throw new RuntimeException("Panic");
                }
            }
            sw.stop();
            allBatchTimes.addAll(batchTimes);
            
            long    bytes;
    
            out.printf("valueSize %d\n\n", valueSize);
            
            StatSeries  batchStats;
            
            batchStats = new StatSeries(batchTimes);
            
            bytes = calcBytes(p, valueSize);
            _Mbps = NetUtil.calcMbps(bytes, sw);
            out.printf("Elapsed           %s\n", sw);
            out.printf("Bytes             %d\n", bytes);
            out.printf("Throughput (Mbps) %f\n", _Mbps);
            out.printf("Sum               %f\n", batchStats.sum());
            out.printf("Max               %f\n", batchStats.max());
            out.printf("50%%               %f\n", batchStats.percentile(50));
            out.printf("95%%               %f\n", batchStats.percentile(90));
            out.printf("90%%               %f\n", batchStats.percentile(95));
            out.printf("99%%               %f\n", batchStats.percentile(99));
            throughputList.add(_Mbps);
        }
        
        StatSeries  allBatchStats;
        
        allBatchStats = new StatSeries(allBatchTimes);
        out.printf("\n\nSum               %f\n\n", allBatchStats.sum());
        out.printf("Max               %f\n\n", allBatchStats.max());
        for (int i = 0; i <= 95; i+= 5) {
            out.printf("%3d%%\t%f\n", i, allBatchStats.percentile(i));
        }
        out.printf("%3d%%\t%f\n", 99, allBatchStats.percentile(99));
        
        out.println(StatSeries.summaryHeaderStringLow());
        out.println(new StatSeries(throughputList).toSummaryStringLow());
    
        out.println("\n\n");
        //processReplicaTimeStats();
    }
    
        public void runTest(BulkThroughputTest test, TestParameters p, int externalReps) throws PutException, RetrievalException {
            List<Double>    throughputList;
            List<Double>    allBatchTimes;
    
            allBatchTimes = new ArrayList<>(externalReps * ((p.maxKey - p.minKey) / p.batchSize + 1));
            
            throughputList = new ArrayList<>(externalReps);
            for (int j = 0; j < externalReps; j++) {
                Stopwatch   sw;
                double      _Mbps;
                int         valueSize;
                List<Double>    batchTimes;
                
                switch (test) {
                case Write: valueSize = values[0].length; break;
                default: valueSize = -1; 
                }
                
                batchTimes = new ArrayList<>((p.maxKey - p.minKey) / p.batchSize + 1);
                
                sw = new SimpleStopwatch();
                for (int i = 0; i < p.repetitions; i++) {
                    switch (test) {
                    case Write: write(p); break;
                    case Read: valueSize = read(p, batchTimes); break;
                    case ReadAsync: valueSize = readAsync(p, batchTimes); break;
                    default: throw new RuntimeException("Panic");
                    }
                }
                sw.stop();
                allBatchTimes.addAll(batchTimes);
                
                long    bytes;
        
                out.printf("valueSize %d\n\n", valueSize);
                
                StatSeries  batchStats;
                
                batchStats = new StatSeries(batchTimes);
                
                bytes = calcBytes(p, valueSize);
                _Mbps = NetUtil.calcMbps(bytes, sw);
                out.printf("Elapsed           %s\n", sw);
                out.printf("Bytes             %d\n", bytes);
                out.printf("Throughput (Mbps) %f\n", _Mbps);
                out.printf("Sum               %f\n", batchStats.sum());
                out.printf("Max               %f\n", batchStats.max());
                out.printf("50%%               %f\n", batchStats.percentile(50));
                out.printf("95%%               %f\n", batchStats.percentile(90));
                out.printf("90%%               %f\n", batchStats.percentile(95));
                out.printf("99%%               %f\n", batchStats.percentile(99));
                throughputList.add(_Mbps);
            }
            
            StatSeries  allBatchStats;
            
            allBatchStats = new StatSeries(allBatchTimes);
            out.printf("\n\nSum               %f\n\n", allBatchStats.sum());
            out.printf("Max               %f\n\n", allBatchStats.max());
            for (int i = 0; i <= 95; i+= 5) {
                out.printf("%3d%%\t%f\n", i, allBatchStats.percentile(i));
            }
            out.printf("%3d%%\t%f\n", 99, allBatchStats.percentile(99));
            
            out.println(StatSeries.summaryHeaderStringLow());
            out.println(new StatSeries(throughputList).toSummaryStringLow());
        
            out.println("\n\n");
            //processReplicaTimeStats();
        }
    }
    
    private long calcBytes(TestParameters p, int valueSize) {
        return (long)p.repetitions * (long)p.numKeys * (long)valueSize;
    }

    private static byte[][] createValues(int numValues, int valueSize) {
        byte[][]    v;
        
        v = new byte[numValues][valueSize];
        for (int i = 0; i < v.length; i++) {
            ThreadLocalRandom.current().nextBytes(v[i]);
        }
        return v;
    }
    
    private Map<String,byte[]> createMap(int minKey, int batchSize) {
        ImmutableMap.Builder<String,byte[]>   mb;
        
        mb = ImmutableMap.builder();
        for (int i = 0; i < batchSize; i++) {
            mb.put(key(minKey + i), values[i]);
        }
        return mb.build();
    }

    private List<String> getReplicas(String key) throws RetrievalException {
        String      replicaValue;
        List<String> replicaList;
        int         i1;
        int         i2;
        String      s;
        
        replicaValue = replicasNSP.get(key);
        i1 = replicaValue.indexOf(':');
        i2 = replicaValue.indexOf("Secondary:");
        s = replicaValue.substring(i1 + 1, i2).trim();
        
        replicaList = new ArrayList<>(3);
        for (String replica : replicaValue.split(",")) {
            replicaList.add(s.trim());
        }
        return replicaList;
    }
    
    private void storeKeyTimes(Map<String, ? extends StoredValue<byte[]>> latestResults, double curElapsed) 
            throws RetrievalException {
        for (String key : latestResults.keySet()) {
            Double  prev;

            prev = keyTimes.get(key);
            if (prev != null && prev > curElapsed) {
                curElapsed = prev;
            }
            prev = keyTimes.put(key, curElapsed);
            if (prev != null) {
                System.err.println("Error: replaced key");
            }
        }
    }
    
    private void storeReplicaTimes(String key, double curElapsed) 
                                    throws RetrievalException {
        List<String>    replicas;
        
        replicas = getReplicas(key);
        for (String replica : replicas) {
            replicaTimes.addValue(replica, curElapsed);
        }
    }
    
    private void processReplicaTimeStats() throws RetrievalException {
        for (String key : keyTimes.keySet()) {
            //out.println(key +"\t"+ keyTimes.get(key));
            storeReplicaTimes(key, keyTimes.get(key));
        }
        for (String replica : replicaTimes.getKeys()) {
            List<Double>    times;
            StatSeries      replicaStats;
            
            times = replicaTimes.getList(replica);
            replicaStats = new StatSeries(times);
            out.printf("%s\t%f\t%f\t%f\n", replica, replicaStats.mean(), replicaStats.median(), replicaStats.max());
            for (int i = 0; i <= 95; i+= 5) {
                out.printf("%3d%%\t%f\n", i, replicaStats.percentile(i));
            }
            out.printf("%3d%%\t%f\n", 99, replicaStats.percentile(99));
        }
    }

    private Set<String> createSet(int minKey, int batchSize) {
        ImmutableSet.Builder<String>   mb;
        
        mb = ImmutableSet.builder();
        for (int i = 0; i < batchSize; i++) {
            mb.add(key(minKey + i));
        }
        return mb.build();
    }
    
    private static final String key(int i) {
        return "key."+ i;
    }
    
    static class TestParameters {
        final int   numKeys;
        final int   batchSize;
        final int   minKey;
        final int   maxKey;
        final int   repetitions;
        
        TestParameters(int numKeys, int batchSize, int minKey, int maxKey, int repetitions) {
            this.numKeys = numKeys;
            this.batchSize = batchSize;
            this.minKey = minKey;
            this.maxKey = maxKey;
            this.repetitions = repetitions;
        }
    }
    
    private static void checkOptions(BulkThroughputOptions options) throws CmdLineException {
        if (options.test == BulkThroughputTest.Write) {
            if (options.valueSize == -1) {
                throw new CmdLineException("valueSize must be specified for Write");
            }
        }
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            BulkThroughputAnalysis          bt;
            BulkThroughputOptions   options;
            CmdLineParser           parser;
            
            options = new BulkThroughputOptions();
            parser = new CmdLineParser(options);
            try {
                parser.parseArgument(args);
                checkOptions(options);
            } catch (CmdLineException cle) {
                System.err.println(cle.getMessage());
                parser.printUsage(System.err);
                return;
            }
            Log.fine(options);
            LWTPoolProvider.createDefaultWorkPools(DefaultWorkPoolParameters.defaultParameters().workUnit(options.clientWorkUnit));
            if (options.verbose) {
                Log.setLevelAll();
            }
            bt = new BulkThroughputAnalysis(SKGridConfiguration.parseFile(options.gridConfig),
                                     System.out, System.err, options);
            for (int i = 0; i < options.parallelThreads; i++) {
                bt.runParallelTest(options.test, 
                       new TestParameters(options.numKeys, options.batchSize, 0, options.numKeys, options.reps), 
                       options.externalReps);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

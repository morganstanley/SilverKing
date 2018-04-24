package com.ms.silverking.cloud.dht.client.apps.test;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.SessionOptions;
import com.ms.silverking.cloud.dht.client.AsyncValueRetrieval;
import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.ConstantVersionProvider;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.NetUtil;
import com.ms.silverking.numeric.StatSeries;
import com.ms.silverking.thread.lwt.DefaultWorkPoolParameters;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

public class BulkThroughput {
    private final PrintStream       out;
    private final PrintStream       err;
    private final DHTClient			dhtClient;
    private final byte[][]          values;
    private TestRunner[]			testRunners;
    private final String			nsName;
    private final NamespaceOptions  nsOptions;
    private final AsynchronousNamespacePerspective<String,byte[]> _asyncNSP;
    private final SynchronousNamespacePerspective<String,byte[]>  _syncNSP;
    private final BulkThroughputOptions options;
    private final SKGridConfiguration gridConfig;    
    private static final String nsBase = "BulkThroughput.";
    private static final String keyPrefix = "key.";
    private static final AtomicInteger  totalMissing = new AtomicInteger();
    
    private static final AtomicInteger	nextThreadID = new AtomicInteger();
    
    public BulkThroughput(SKGridConfiguration gridConfig, PrintStream out, PrintStream err, 
                          BulkThroughputOptions options) throws ClientException, IOException {
        NamespaceOptions    _nsOptions;
        
    	this.gridConfig = gridConfig;
        nsName = nsBase + options.id;
        this.out = out;
        this.err = err;
        this.options = options;
        dhtClient = new DHTClient();
        values = createValues(options.batchSize, options.valueSize);
        
        DHTSession	session;
        Namespace	ns;
        
        try {
            //session = dhtClient.openSession(gridConfig);
            session = dhtClient.openSession(new SessionOptions(gridConfig, options.server));
            _nsOptions = session.getDefaultNamespaceOptions()
                    .defaultPutOptions(session.getDefaultPutOptions()
                                        .compression(options.compression)
                                        .checksumType(options.checksumType));
            if (options.consistencyProtocol != null) {
                _nsOptions = _nsOptions.consistencyProtocol(options.consistencyProtocol);
            }
            if (options.storageType != null) {
                _nsOptions = _nsOptions.storageType(options.storageType);
            }
            if (options.nsVersionMode != null) {
                _nsOptions = _nsOptions.versionMode(options.nsVersionMode);
            }
            
            nsOptions = _nsOptions;
//            System.out.println(nsOptions + "\n");
            try {
                ns = session.getNamespace(nsName);
            } catch (RuntimeException nce) {
                ns = null;
            }
            if (ns == null) {
                ns = session.createNamespace(nsName, nsOptions);
            }
//            System.out.println(nsName +"\n"+ ns.getOptions() + "\n");
		} catch (Exception e) {
			throw new RuntimeException (e);
		}

        _syncNSP = ns.openSyncPerspective(String.class, byte[].class);
        _asyncNSP = ns.openAsyncPerspective(String.class, byte[].class);
        
//        System.out.println(_syncNSP.getOptions() + "\n");
//        _syncNSP.setDefaultVersionProvider(new ConstantVersionProvider(System.nanoTime()));
//        System.out.println(_syncNSP.getOptions() + "\n");
    }
    
    class TestRunner implements Runnable {
        private final BulkThroughputTest    test;
        private final TestParameters        p;
        private final int                   externalReps;
        private final Thread				_thread;
        private final int					id;
        private AsynchronousNamespacePerspective<String,byte[]> asyncNSP;
        private SynchronousNamespacePerspective<String,byte[]>  syncNSP;

        
        TestRunner(BulkThroughputTest test, TestParameters p, int externalReps) {
            this.test = test;
            this.p = p;
            this.externalReps = externalReps;
            _thread = new Thread(this);
        	id = nextThreadID.getAndIncrement();
        	testRunners[id] = this;
        	
            if (options.dedicatedNamespaces) {
                Namespace   ns;
                DHTSession	session;
                
                try {
                    session = dhtClient.openSession(gridConfig);
                    System.out.printf("Opened session: %s\n", session);
    				//ns = session.createNamespace(nsName, nsOptions);
                    ns = session.getNamespace(nsName);
    			} catch (Exception e) {
    				throw new RuntimeException (e);
    			}
	            syncNSP = ns.openSyncPerspective(String.class, byte[].class);
	            asyncNSP = ns.openAsyncPerspective(String.class, byte[].class);
            } else {
            	syncNSP = _syncNSP;
            	asyncNSP = _asyncNSP;
            }
        }
        
        public void start() {
            _thread.start();
        }
        
        public void run() {
            try {
                runTest(test, p, externalReps, id, syncNSP, asyncNSP);
            } catch (PutException pe) {
                pe.printStackTrace();
                System.err.println(pe.getDetailedFailureMessage());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        public void waitForCompletion() {
        	try {
        		_thread.join();
        	} catch (InterruptedException ie) {
        	}
        }
    }
    
    public void runParallelTests(BulkThroughputOptions options) throws PutException, RetrievalException {
    	Stopwatch	sw;
    	double		iops;
    	
    	testRunners = new TestRunner[options.parallelThreads];
        for (int i = 0; i < options.parallelThreads; i++) {
            runParallelTest(options.test, 
                   new TestParameters(options.numKeys, options.batchSize, 0, options.numKeys, options.reps), 
                   options.externalReps);
        }
    	sw = new SimpleStopwatch();
    	out.printf("\n *** Starting ***\n");
        for (int i = 0; i < options.parallelThreads; i++) {
        	testRunners[i].start();
        }
        for (int i = 0; i < options.parallelThreads; i++) {
        	testRunners[i].waitForCompletion();
        }
        sw.stop();
        
        long bytes = (long)options.reps * (long)options.numKeys * (long)options.valueSize;
        double _Mbps = NetUtil.calcMbps(bytes, sw);
        iops = (double)(options.numKeys * options.reps * options.parallelThreads) / sw.getElapsedSeconds();
        double timePerKey = 1 / iops;
        out.printf("\n *** Aggregate ***\n");
        out.printf("Elapsed           %s\n", sw);
        out.printf("Throughput (Mbps) %f\n", _Mbps);
        out.printf("Throughput (IOPS) %f\n", iops);
        out.printf("Time / key        %f s (%f ms)\n", timePerKey, timePerKey*1000);
    }
    
    public void runParallelTest(BulkThroughputTest test, TestParameters p, int externalReps) throws PutException, RetrievalException {
        new TestRunner(test, p, externalReps);
    }
    
    public void runTest(BulkThroughputTest test, TestParameters p, int externalReps, int threadID,
    		SynchronousNamespacePerspective<String,byte[]> syncNSP, AsynchronousNamespacePerspective<String,byte[]> asyncNSP) throws PutException, RetrievalException {
        List<Double>    throughputList;
        List<Double>    allBatchTimes;
        String			context;

        context = keyPrefix + threadID +".";
        
        int size = externalReps * ((p.maxKey - p.minKey) / p.batchSize + 1);
        allBatchTimes = new ArrayList<>(size);

        System.out.println("max: " + p.maxKey);
        System.out.println("min: " + p.minKey);
        System.out.println("batchSize: " + p.batchSize);
        System.out.println("size: " + size);
        
        
        
        throughputList = new ArrayList<>(externalReps);
        for (int j = 0; j < externalReps; j++) {
            Stopwatch   sw;
            double      _Mbps;
            int         valueSize;
            List<Double>    batchTimes;
            double		iops;
            
            switch (test) {
            case Write: valueSize = values[0].length; break;
            default: valueSize = -1; 
            }
            
            batchTimes = new ArrayList<>((p.maxKey - p.minKey) / p.batchSize + 1);
            
            sw = new SimpleStopwatch();
            for (int i = 0; i < p.repetitions; i++) {
                switch (test) {
                case Write: write(p, batchTimes, context, syncNSP); break;
                case Read: valueSize = read(p, batchTimes, context, syncNSP); break;
                case ReadAsync: valueSize = readAsync(p, context, asyncNSP); break;
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
            iops = (double)(p.numKeys  * p.repetitions) / sw.getElapsedSeconds();
            out.printf("Elapsed           %s\n", sw);
            out.printf("Bytes             %d\n", bytes);
            out.printf("Throughput (Mbps) %f\n", _Mbps);
            out.printf("Throughput (IOPS) %f\n", iops);
            out.printf("Sum               %f\n", batchStats.sum());
            out.printf("Max               %f\n", batchStats.max());
            out.printf("50%%               %f\n", batchStats.percentile(90));
            out.printf("90%%               %f\n", batchStats.percentile(90));
            out.printf("95%%               %f\n", batchStats.percentile(95));
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
        
        out.println(StatSeries.summaryHeaderString());
        out.println(new StatSeries(throughputList).toSummaryString());
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
    
    public void write(TestParameters p, List<Double> batchTimes, String context, SynchronousNamespacePerspective<String,byte[]> syncNSP) throws PutException {
        int k;
        int lastDisplay;

        Stopwatch batchSW = new SimpleStopwatch();
        Map<String,byte[]> keysAndVals;
        k = p.minKey;
        lastDisplay = p.minKey;
        int i = 0;
		int batchMultiple = p.maxKey / p.batchSize / 10;
        while (k <= p.maxKey - p.batchSize) {
    		if (i++ % batchMultiple == 0)
    			System.out.println("batch: " + i);
        	
            int batchSize;
            
            batchSize = Math.min(p.batchSize, p.maxKey - k + 1);
            //out.printf("put %d\n", k, batchSize);
            keysAndVals = createMap(context, k, batchSize);
            batchSW.reset();
            syncNSP.put(keysAndVals);
            batchSW.stop();
            batchTimes.add(batchSW.getElapsedSeconds());
            k += batchSize;
            if (options.verbose && k - lastDisplay > options.displayInterval) {
                out.printf("%d\t%s\n", k, new Date());
                lastDisplay = k;
            }
        }
    }
    
    private Map<String,byte[]> createMap(String context, int minKey, int batchSize) {
        ImmutableMap.Builder<String,byte[]>   mb;
        
        mb = ImmutableMap.builder();
        for (int i = 0; i < batchSize; i++) {
            mb.put(key(context, minKey + i), values[i]);
        }
        return mb.build();
    }
    
    public int read(TestParameters p, List<Double> batchTimes, String context, SynchronousNamespacePerspective<String,byte[]> syncNSP) throws RetrievalException {
        int k;
        int readSize;
        Stopwatch   batchSW;
        int lastDisplay;
        
        batchSW = new SimpleStopwatch();
        readSize = -1;
        k = p.minKey;
        lastDisplay = p.minKey;
        while (k <= p.maxKey - p.batchSize) {
            int batchSize;
            Map<String,byte[]>  values;
            Set<String> keys;
            byte[]  value;
            
            batchSize = Math.min(p.batchSize, p.maxKey - p.minKey + 1);
            keys = createSet(context, k, batchSize);
            batchSW.reset();
            values = syncNSP.get(keys);
            batchSW.stop();
            if (options.verifyValues) {
                verifyValues(keys, values);
            }
            batchTimes.add(batchSW.getElapsedSeconds());
            k += batchSize;
            value = values.get(key(context, p.minKey));
            if (value != null) {
                readSize += value.length;
            }
            if (options.verbose && k - lastDisplay >= options.displayInterval) {
                out.printf("%d\t%s\n", k, new Date());
                lastDisplay = k;
            }
        }
        return readSize;
    }
    
    private void verifyValues(Set<String> keys, Map<String, byte[]> values) {
        for (String key : keys) {
            byte[]  value;
            
            value = values.get(key);
            //out.printf("%s\t%s\n", key, value == null ? "<missing>" : value.length);
            if (value == null) {
                out.printf("Missing:\t%s\n", key);
                totalMissing.incrementAndGet();
            }
        }
    }

    public int readAsync(TestParameters p, String context, AsynchronousNamespacePerspective<String,byte[]> asyncNSP) throws RetrievalException {
        int k;
        int readSize;
        AsyncValueRetrieval<String,byte[]>  prevAsyncRetrieval;
        Map<String,byte[]>  values;
        
        prevAsyncRetrieval = null;
        readSize = -1;
        k = p.minKey;
        while (k <= p.maxKey) {
            int batchSize;
            AsyncValueRetrieval<String,byte[]>  asyncRetrieval;
            
            batchSize = Math.min(p.batchSize, p.maxKey - p.minKey + 1);
            asyncRetrieval = asyncNSP.get(createSet(context, p.minKey, batchSize));
            if (prevAsyncRetrieval != null) {
                prevAsyncRetrieval.waitForCompletion();
                values = prevAsyncRetrieval.getValues();
                readSize = values.get(key(context, p.minKey)).length;
            }
            prevAsyncRetrieval = asyncRetrieval;
            k += batchSize;
        }
        if (prevAsyncRetrieval != null) {
            prevAsyncRetrieval.waitForCompletion();
            values = prevAsyncRetrieval.getValues();
            readSize = values.get(key(context, p.minKey)).length;
        }
        return readSize;
    }
    
    private Set<String> createSet(String context, int minKey, int batchSize) {
        ImmutableSet.Builder<String>   mb;
        
        mb = ImmutableSet.builder();
        for (int i = 0; i < batchSize; i++) {
            mb.add(key(context, minKey + i));
        }
        return mb.build();
    }
    
    private static final String key(String context, int i) {
        return context + i;
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
            BulkThroughput          bt;
            BulkThroughputOptions   options;
            CmdLineParser           parser;
            
            //for (int i = 0; i < args.length; i++) {
            //	System.out.printf("%d\t%s\n", i, args[i]);
            //}
            
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
            //Log.warning(options);
            
            LWTPoolProvider.createDefaultWorkPools(DefaultWorkPoolParameters.defaultParameters().workUnit(options.clientWorkUnit));
            if (options.debug) {
                Log.setLevelAll();
            }
            System.out.printf("dedicatedNamespaces: %s\n", options.dedicatedNamespaces);
            bt = new BulkThroughput(SKGridConfiguration.parseFile(options.gridConfig),
                                     System.out, System.err, options);
            bt.runParallelTests(options);
            if (options.verifyValues) {
                System.out.printf("totalMissing:\t%d\n", totalMissing.get());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

package com.ms.silverking.test.pingpong;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.ms.silverking.cloud.dht.NamespacePerspectiveOptions;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.SessionOptions;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.log.Log;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.thread.lwt.DefaultWorkPoolParameters;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

public class PingMultiPong {
    private final Mode  mode;
    private final int   iterations;
    private final boolean   verbose;
    private final int   id;
    private final int   numServers;
    private final DHTClient dhtClient;
    private final SynchronousNamespacePerspective<String,byte[]>    syncNSP;
    private final PutOptions    defaultPutOptions;
    private final int delayMS;
    private final int threadsPerServer;
    
    private static final int   pingPongWorkUnit = 10;
    
    public PingMultiPong(PingPongOptions options) throws IOException, ClientException {
        DHTSession  session;
        Stopwatch   sw;
        int         outerReps;
        NamespacePerspectiveOptions<String,byte[]> nspOptions;

        this.mode = options.mode;
        this.iterations = options.iterations;
        this.verbose = options.verbose;
        this.delayMS = options.delay * 1000;
        this.id = options.id;
        this.threadsPerServer = options.threadsPerServer;
        this.numServers = options.numServers;
        dhtClient = new DHTClient();
        session = dhtClient.openSession(new SessionOptions(SKGridConfiguration.parseFile(options.gridConfig), options.server));
        if (session == null) {
            throw new RuntimeException("null session");
        }
        nspOptions = session.getNamespace(options.namespace).getDefaultNSPOptions(String.class, byte[].class);
        if (options.checksumType != null) {
            defaultPutOptions = session.getDefaultNamespaceOptions().getDefaultPutOptions().checksumType(ChecksumType.valueOf(options.checksumType));
        } else {
            defaultPutOptions = session.getDefaultNamespaceOptions().getDefaultPutOptions();
        }
        //System.out.println("nspOptions: "+ nspOptions);
        syncNSP = session.openSyncNamespacePerspective(options.namespace, nspOptions);
    }
    
    public void clientIteration(String pingKey, String pongKeyBase, long version) throws PutException, RetrievalException {
        Set<String> pongKeys;
        
        pongKeys = new HashSet<>(numServers);
        for (int i = 0; i < numServers; i++) {
            for (int j = 0; j < threadsPerServer; j++) {
                pongKeys.add(pongKeyBase +"."+ i +"."+ j);
            }
        }
        if (verbose) {
            System.out.println("Put: "+ pingKey);
        }
        if (delayMS != 0) {
            if (verbose) {
                System.out.print("Sleeping...");
            }
            ThreadUtil.sleep(delayMS);
            if (verbose) {
                System.out.println("Awake.");
            }
        }
        try {
            syncNSP.put(pingKey, pingKey.getBytes(), defaultPutOptions.version(version));
        } catch (PutException pe) {
            System.out.println("ignoring put exception");
        }
        if (verbose) {
            System.out.println("WaitFor: "+ pongKeyBase);
        }
        syncNSP.waitFor(pongKeys, syncNSP.getOptions().getDefaultWaitOptions().versionConstraint(VersionConstraint.exactMatch(version)));
        if (verbose) {
            System.out.println("Received: "+ pongKeyBase);
        }
    }
    
    public void serverIteration(String pingKey, String pongKeyBase, long version, int threadID) throws PutException, RetrievalException {
        String  pongKey;
        
        pongKey = pongKeyBase +"."+ id +"."+ threadID;
        if (verbose) {
            System.out.println("WaitFor: "+ pingKey);
        }
        syncNSP.waitFor(pingKey, syncNSP.getOptions().getDefaultWaitOptions().versionConstraint(VersionConstraint.exactMatch(version)));
        if (verbose) {
            System.out.println("Received: "+ pingKey);
        }
        syncNSP.put(pongKey, pongKey.getBytes(), defaultPutOptions.version(version));
        if (verbose) {
            System.out.println("Put: "+ pongKey);
        }
    }
    
    public void runTest() throws PutException, RetrievalException {
        ServerRunner[]  serverRunners;

        serverRunners = new ServerRunner[threadsPerServer];
        switch (mode) {
        case Client: _runTest(0); break;
        case Server: 
            for (int i = 0; i < threadsPerServer; i++) {
                serverRunners[i] = new ServerRunner(i);
            }
            for (int i = 0; i < threadsPerServer; i++) {
                serverRunners[i].waitForCompletion();
            }
            break;
        default: throw new RuntimeException("panic");
        }
    }
    
    private void _runTest(int threadID) throws PutException, RetrievalException {
        Stopwatch   sw;
        
        System.out.println("Mode: "+ mode);
        sw = new SimpleStopwatch();
        for (int i = 0; i < iterations; i++) {
            String  pingKey;
            String  pongKeyBase;
            long    version;
            
            version = 0;
            pingKey = "Ping."+ i;
            pongKeyBase = "Pong."+ i;
            /*
            version = i;
            pingKey = "Ping";
            pongKey = "Pong";
            */
            if (verbose) {
                System.out.println("Iteration");
            }
            switch (mode) {
            case Client: clientIteration(pingKey, pongKeyBase, version); break;
            case Server: serverIteration(pingKey, pongKeyBase, version, threadID); break;
            default: throw new RuntimeException("panic");
            }
        }
        sw.stop();
        Log.warning("Elapsed:\t"+ sw.getElapsedSeconds());
    }
    
    
    class ServerRunner implements Runnable {
        private final int   threadID;
        private volatile boolean     complete;
        
        ServerRunner(int threadID) {
            this.threadID = threadID;
            new Thread(this).start();
        }
        
        public void run() {
            try {
                _runTest(threadID);
                complete = true;
                synchronized (this) {
                    this.notifyAll();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        void waitForCompletion() {
            synchronized (this) {
                while (!complete) {
                    try {
                        this.wait();
                    } catch (InterruptedException ie) {
                    }
                }
            }
        }
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            PingMultiPong        pingPong;
            PingPongOptions options;
            CmdLineParser   parser;
            
            LWTPoolProvider.createDefaultWorkPools(DefaultWorkPoolParameters.defaultParameters().workUnit(pingPongWorkUnit));
            options = new PingPongOptions();
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
                //Log.setLevelAll();
            }
            pingPong = new PingMultiPong(options);
            pingPong.runTest();
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

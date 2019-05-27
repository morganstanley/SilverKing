package com.ms.silverking.test.pingpong;

import java.io.IOException;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.ms.silverking.cloud.dht.NamespacePerspectiveOptions;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.SessionOptions;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.WaitOptions;
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

public class PingPong {
    private final Mode  mode;
    private final int   iterations;
    private final boolean   verbose;
    private final DHTClient dhtClient;
    private final SynchronousNamespacePerspective<String,byte[]>    syncNSP;
    private final PutOptions    defaultPutOptions;
    private final int delayMS;
    private final boolean   writeOnceNS;
    
    private static final int   pingPongWorkUnit = 10;
    
    public PingPong(PingPongOptions options) throws IOException, ClientException {
        DHTSession  session;
        Stopwatch   sw;
        int         outerReps;
        NamespacePerspectiveOptions<String,byte[]> nspOptions;

        this.mode = options.mode;
        this.iterations = options.iterations;
        this.verbose = options.verbose;
        this.delayMS = options.delay * 1000;
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
        writeOnceNS = true; // FIXME
    }
    
    private WaitOptions getWaitOptions(long version) {
        if (!writeOnceNS) {
            return syncNSP.getOptions().getDefaultWaitOptions().versionConstraint(VersionConstraint.exactMatch(version));
        } else {
            return syncNSP.getOptions().getDefaultWaitOptions();
        }
    }
    
    private PutOptions getPutOptions(long version) {
        if (!writeOnceNS) {
            return defaultPutOptions.version(version);
        } else {
            return defaultPutOptions;
        }
    }
    
    public void clientIteration(String pingKey, String pongKey, long version) throws PutException, RetrievalException {
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
        syncNSP.put(pingKey, pingKey.getBytes(), getPutOptions(version));
        if (verbose) {
            System.out.println("WaitFor: "+ pongKey);
        }
        syncNSP.waitFor(pongKey, getWaitOptions(version));
        if (verbose) {
            System.out.println("Received: "+ pongKey);
        }
    }
    
    public void serverIteration(String pingKey, String pongKey, long version) throws PutException, RetrievalException {
        if (verbose) {
            System.out.println("WaitFor: "+ pingKey);
        }
        syncNSP.waitFor(pingKey, getWaitOptions(version));
        if (verbose) {
            System.out.println("Received: "+ pingKey);
        }
        syncNSP.put(pongKey, pongKey.getBytes(), getPutOptions(version));
        if (verbose) {
            System.out.println("Put: "+ pongKey);
        }
    }
    
    public void runTest() throws PutException, RetrievalException {
        Stopwatch   sw;
        
        System.out.println("Mode: "+ mode);
        sw = new SimpleStopwatch();
        for (int i = 0; i < iterations; i++) {
            String  pingKey;
            String  pongKey;
            long    version;
            
            version = 0;
            pingKey = "Ping."+ i;
            pongKey = "Pong."+ i;
            /*
            version = i;
            pingKey = "Ping";
            pongKey = "Pong";
            */
            if (verbose) {
                System.out.println("Iteration");
            }
            switch (mode) {
            case Client: clientIteration(pingKey, pongKey, version); break;
            case Server: serverIteration(pingKey, pongKey, version); break;
            default: throw new RuntimeException("panic");
            }
        }
        sw.stop();
        Log.warning("Elapsed:\t"+ sw.getElapsedSeconds());
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            PingPong        pingPong;
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
            pingPong = new PingPong(options);
            pingPong.runTest();
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

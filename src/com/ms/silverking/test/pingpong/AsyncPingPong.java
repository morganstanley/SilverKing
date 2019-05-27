package com.ms.silverking.test.pingpong;

import java.io.IOException;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.ms.silverking.cloud.dht.NamespacePerspectiveOptions;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.SessionOptions;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.client.AsyncPut;
import com.ms.silverking.cloud.dht.client.AsyncRetrieval;
import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.log.Log;
import com.ms.silverking.thread.lwt.DefaultWorkPoolParameters;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

public class AsyncPingPong {
    private final Mode  mode;
    private final int   iterations;
    private final boolean   verbose;
    private final DHTClient dhtClient;
    private final AsynchronousNamespacePerspective<String,byte[]>    asyncNSP;
    private final PutOptions    defaultPutOptions;
    
    private static final int   pingPongWorkUnit = 10;
    
    public AsyncPingPong(PingPongOptions options) throws IOException, ClientException {
        DHTSession  session;
        NamespacePerspectiveOptions<String,byte[]> nspOptions;

        this.mode = options.mode;
        this.iterations = options.iterations;
        this.verbose = options.verbose;
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
        asyncNSP = session.openAsyncNamespacePerspective(options.namespace, nspOptions);
    }
    
    public void clientIteration(String pingKey, String pongKey, long version) throws PutException, RetrievalException {
        AsyncPut<String>    asyncPut;
        AsyncRetrieval<String,byte[]>   asyncWaitFor;
        
        if (verbose) {
            System.out.println("Put: "+ pingKey);
        }
        asyncPut = asyncNSP.put(pingKey, pingKey.getBytes(), defaultPutOptions.version(version));
        if (verbose) {
            System.out.println("WaitFor: "+ pongKey);
        }
        asyncWaitFor = asyncNSP.waitFor(pongKey, asyncNSP.getOptions().getDefaultWaitOptions().versionConstraint(VersionConstraint.exactMatch(version)));
        asyncPut.waitForCompletion();
        asyncWaitFor.waitForCompletion();
        if (verbose) {
            System.out.println("Received: "+ pongKey);
        }
    }
    
    public void serverIteration(String pingKey, String pongKey, long version) throws PutException, RetrievalException {
        AsyncPut<String>    asyncPut;
        AsyncRetrieval<String,byte[]>   asyncWaitFor;
        
        if (verbose) {
            System.out.println("WaitFor: "+ pingKey);
        }
        asyncWaitFor = asyncNSP.waitFor(pingKey, asyncNSP.getOptions().getDefaultWaitOptions().versionConstraint(VersionConstraint.exactMatch(version)));
        asyncWaitFor.waitForCompletion();
        if (verbose) {
            System.out.println("Received: "+ pingKey);
        }
        asyncPut = asyncNSP.put(pongKey, pongKey.getBytes(), defaultPutOptions.version(version));
        asyncPut.waitForCompletion();
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
            AsyncPingPong        pingPong;
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
            pingPong = new AsyncPingPong(options);
            pingPong.runTest();
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

package com.ms.silverking.cloud.dht.client.apps.test;

import java.io.IOException;
import java.io.PrintStream;

import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.StorageType;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

public class PerspectiveTest {
    private final PrintStream out;
    private final PrintStream err;
    private final DHTSession  session;
    private final Namespace   ns;

    private static final String nsBase = "Perspective.";
    
    public PerspectiveTest(SKGridConfiguration gridConfig, PrintStream out, PrintStream err) throws ClientException, IOException {
        String      nsName;
        NamespaceOptions    nsOptions;
        
        nsName = nsBase + System.currentTimeMillis();
        this.out = out;
        this.err = err;
        session = new DHTClient().openSession(gridConfig);
        nsOptions = session.getDefaultNamespaceOptions()
                        .storageType(StorageType.RAM)
                        .consistencyProtocol(ConsistencyProtocol.LOOSE)
                        .defaultPutOptions(session.getDefaultPutOptions()
                                            .compression(Compression.NONE)
                                            .checksumType(ChecksumType.NONE));
        ns = session.createNamespace(nsName, nsOptions);
        //asyncNSP = ns.openAsyncPerspective(new NamespacePerspectiveOptions<>(String.class, byte[].class));
    }
    

    public void createPerspectives(int numPerspectives) {
        Stopwatch   runSW;
        
        runSW = new SimpleStopwatch();
        for (int i = 0; i < numPerspectives; i++) {
            ns.openAsyncPerspective(String.class, byte[].class);
        }
        runSW.stop();
        out.printf("Num Perspectives: %d\n", numPerspectives);
        out.printf("Elapsed: %f\n", runSW.getElapsedSeconds());
        out.printf("Perspectives / s: %f\n", (double)numPerspectives / runSW.getElapsedSeconds());
        out.printf("s / Perspective:  %f\n", runSW.getElapsedSeconds() / (double)numPerspectives);
        out.printf("\n");
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            if (args.length != 2) {
                System.err.println("Usage: <gridConfig> <numPerspectives>");
            } else {
                PerspectiveTest pt;
                String          gridConfig;
                int             numPerspectives;
             
                gridConfig = args[0];
                numPerspectives = Integer.parseInt(args[1]);
                pt = new PerspectiveTest(SKGridConfiguration.parseFile(gridConfig),
                                         System.out, System.err);
                pt.createPerspectives(numPerspectives);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

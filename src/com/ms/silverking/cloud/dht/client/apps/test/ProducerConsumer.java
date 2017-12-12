package com.ms.silverking.cloud.dht.client.apps.test;

import java.io.IOException;

import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.StorageType;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.WaitOptions;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.StoredValue;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

public class ProducerConsumer {
    private final Mode  mode;
    private final SynchronousNamespacePerspective<String,String>  syncNSP;
    private final int	delay;
    private final int	displayUnit;
    
    private static final int    defaultDisplayUnit = 1000;
    
    private static final String myKey = "k";
    
    private enum Mode {Producer, Consumer};
    
    public ProducerConsumer(SKGridConfiguration gc, String id, Mode mode, int delay) throws ClientException, IOException {
        DHTSession  session;
        Namespace   ns;
        WaitOptions	waitOptions;
        
        this.mode = mode;
        this.delay = delay;
        displayUnit = delay == 0 ? defaultDisplayUnit : 1;
        session = new DHTClient().openSession(gc);
        System.out.print("Creating namespace: "+ id);
        waitOptions = session.getDefaultNamespaceOptions().getDefaultWaitOptions();
        //waitOptions = waitOptions.opTimeoutController(new WaitForTimeoutController(10));
        ns = session.createNamespace(id, session.getDefaultNamespaceOptions()
                                    .storageType(StorageType.RAM)
                                    .versionMode(NamespaceVersionMode.SEQUENTIAL).defaultWaitOptions(waitOptions));
        System.out.println("...created");
        syncNSP = ns.openSyncPerspective(String.class, String.class);
    }
    
    public void run(int items) throws PutException, RetrievalException {
        Stopwatch   sw;
        
        sw = new SimpleStopwatch();
        for (int i = 1; i <= items; i++) {
            switch (mode) {
            case Producer:
                produce(i);
                break;
            case Consumer:
                consume(i);
                break;
            default:
                throw new RuntimeException("panic");
            }
            ThreadUtil.sleep(delay);
        }
        sw.stop();
        System.out.println(sw);
    }
    
    void consume(int i) throws RetrievalException {
        StoredValue<String> sv;
        
        if (i % displayUnit == 0) {
            System.out.print("WaitingFor: "+ i);
        }
        sv = syncNSP.waitFor(myKey, syncNSP.getOptions().getDefaultWaitOptions().versionConstraint(VersionConstraint.exactMatch(i)));
        if (i % displayUnit == 0) {
            System.out.println("...received: "+ i);
            System.out.println(sv.getValue());
        }
    }

    void produce(int i) throws PutException {
        if (i % displayUnit == 0) {
            System.out.print("Producing: "+ i);
        }
        syncNSP.put(myKey, "v"+ i, syncNSP.getNamespace().getOptions().getDefaultPutOptions().version(i));
        if (i % displayUnit == 0) {
            System.out.println("...produced");
        }
    }

    public static void main(String[] args) {
        try {
            if (args.length != 4 && args.length != 5) {
                System.out.println("args: <gridConfig> <id> <mode [Producer|Consumer]> <items> [delay]");
            } else {
                ProducerConsumer    pc;
                Mode    mode;
                int     items;
                SKGridConfiguration   gc;
                String  id;
                int		delay;

                gc = SKGridConfiguration.parseFile(args[0]);
                id = args[1];
                mode = Mode.valueOf(args[2]);
                items = Integer.parseInt(args[3]);
                if (args.length == 5) {
                	delay = Integer.parseInt(args[4]);
                } else {
                	delay = 0;
                }
                pc = new ProducerConsumer(gc, id, mode, delay);
                pc.run(items);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

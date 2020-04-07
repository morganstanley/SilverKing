package com.ms.silverking.cloud.dht.client;

import java.io.IOException;
import java.util.concurrent.Semaphore;

import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;

public class TestCallbackDHT implements AsyncOperationListener {    
    private Semaphore    s;
    
    public void runTest(SKGridConfiguration gridConfig, int iterations) {
        try {
            Namespace   ns;
            AsynchronousNamespacePerspective<String, String>    asyncNSP;
            AsyncPut<String>    asyncPut;
            AsyncSingleValueRetrieval<String,String>    asyncGet;

            s = new Semaphore(0);
            ns = new DHTClient().openSession(gridConfig).createNamespace("MyNamespace" + System.currentTimeMillis());
            asyncNSP = ns.openAsyncPerspective(String.class, String.class);
            for (int i = 0; i < iterations; i++) {
                String  key;
                
                System.out.printf("Iteration %d\n", i);
                System.out.println("Calling put");
                key = "key"+ i;
                asyncPut = asyncNSP.put(key, "value");
                System.out.println("Adding listener");
                asyncPut.addListener(this, OperationState.SUCCEEDED, OperationState.FAILED, OperationState.INCOMPLETE);
                //asyncPut.addListener(this);
                System.out.println("Acquiring semaphore");
                s.acquire();
                s.drainPermits();
                System.out.println("Calling get");
                asyncGet = asyncNSP.get(key);
                System.out.println("Adding listener");
                asyncGet.addListener(this);
                System.out.println("Acquiring semaphore");
                s.acquire();
                System.out.printf("%s\n", asyncGet.getValue());
            }
            System.out.println("Exiting test");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void asyncOperationUpdated(AsyncOperation asyncOperation) {
        System.out.printf("In asyncOperationUpdated %s\n", Thread.currentThread().getName());
        if (asyncOperation.getState() == OperationState.FAILED) {
            System.out.printf("Operation failed: %s\n", asyncOperation);
        } else if (asyncOperation.getState() == OperationState.INCOMPLETE) {
            System.out.printf("Operation incomplete: %s\n", asyncOperation);
        } else {
            System.out.printf("Complete %s\n", asyncOperation);
            s.release();
        }
    }
    
    public static void main(String[] args) throws IOException {
        try {
            new TestCallbackDHT().runTest( SKGridConfiguration.parseFile(args[0]), Integer.parseInt(args[1]) );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

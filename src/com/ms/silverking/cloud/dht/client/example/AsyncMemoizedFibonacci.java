package com.ms.silverking.cloud.dht.client.example;

import java.io.IOException;

import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.WaitForCompletionException;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;

/**
 * Simple local-only demonstration of how SilverKing can be used to support 
 * distributed memoization. 
 * 
 * An actual distributed implementation would use similar techniques integrated
 * with a distributed processing environment.
 * 
 * This implementation uses asynchronous puts to store the memoized results.
 * This allows the result to be computed without delay while allowing
 * future runs to benefit from the memoization.
 * 
 * (This is not intended to demonstrate the fastest method of computing the nth
 *  value of the Fibonacci sequence, or the best way to benefit from memoization.)
 */
public class AsyncMemoizedFibonacci {        
    private final Namespace   ns;
    private final SynchronousNamespacePerspective<Integer, Integer>   syncNSP;
    private final AsynchronousNamespacePerspective<Integer, Integer>   asyncNSP;
    
    private static final String fibNamespace = "_AsyncMemoizedFibonacci";
    
    public AsyncMemoizedFibonacci(SKGridConfiguration gridConfig) throws ClientException, IOException {
        ns = new DHTClient().openSession(gridConfig).getNamespace(fibNamespace);
        asyncNSP = ns.openAsyncPerspective(Integer.class, Integer.class);
        syncNSP  = ns.openSyncPerspective(Integer.class, Integer.class);
        syncNSP.put(0, 0);
        syncNSP.put(1, 1);
    }
    
    public int fibonacci(int n) throws ClientException {
        Integer fn;
        
        System.out.printf("Entering fibonacci(%d)\n", n);
        fn = syncNSP.get(n);
        if (fn != null) {
            System.out.printf("Found memoized value\n");
            return fn;
        } else {
            System.out.printf("Computing value\n");
            fn = fibonacci(n - 1) + fibonacci(n - 2);
            asyncNSP.put(n, fn);
            System.out.printf("Commenced storage of fibonacci(%d) = %d\n", n, fn);
        }
        System.out.printf("Returning fibonacci(%d) = %d\n", n, fn);
        return fn;
    }
    
    public void waitForAsyncWrites() throws WaitForCompletionException {
        asyncNSP.waitForActiveOps();
    }
    
    private static void usage() {
        System.out.println("args: <gridConfig> <n>");
    }
    
    public static void main(String[] args) {
        try {
            if (args.length != 2) {
                usage();
            } else {
                AsyncMemoizedFibonacci  fib;
                SKGridConfiguration  gridConfig;
                int                n;
                int                fn;

                gridConfig = SKGridConfiguration.parseFile(args[0]);
                n = Integer.parseInt(args[1]);
                if (n < 1) {
                    System.out.printf("n must be >= 1 for this demo\n");
                } else {
                    fib = new AsyncMemoizedFibonacci(gridConfig);
                    fn = fib.fibonacci(n);
                    System.out.printf("fibonacci(%d) = %d", n, fn);
                    System.out.printf("\nWaiting for async writes to complete...\n");
                    fib.waitForAsyncWrites();
                    System.out.printf("Async writes are complete\n");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

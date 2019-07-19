package com.ms.silverking.cloud.dht.client.example;

import java.io.IOException;

import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;

/**
 * Simple local-only demonstration of how SilverKing can be used to support 
 * distributed memoization. 
 * 
 * An actual distributed implementation would use similar techniques integrated
 * with a distributed processing environment.
 * 
 * (This is not intended to demonstrate the fastest method of computing the nth
 *  value of the Fibonacci sequence.)
 */
public class MemoizedFibonacci {        
    private SynchronousNamespacePerspective<Integer, Integer>   syncNSP;
    
    private static final String fibNamespace = "_MemoizedFibonacci";
    
    public MemoizedFibonacci(SKGridConfiguration gridConfig) throws ClientException, IOException {
        syncNSP = new DHTClient().openSession(gridConfig)
                .openSyncNamespacePerspective(fibNamespace, Integer.class, Integer.class);
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
            syncNSP.put(n, fn);
            System.out.printf("Stored fibonacci(%d) = %d\n", n, fn);
        }
        System.out.printf("Returning fibonacci(%d) = %d\n", n, fn);
        return fn;
    }
    
    private static void usage() {
        System.out.println("args: <gridConfig> <n>");
    }
    
    public static void main(String[] args) {
        try {
            if (args.length != 2) {
                usage();
            } else {
                MemoizedFibonacci  fib;
                SKGridConfiguration gridConfig;
                int                n;
                int                fn;
                
                gridConfig = SKGridConfiguration.parseFile(args[0]);
                n = Integer.parseInt(args[1]);
                if (n < 1) {
                    System.out.printf("n must be >= 1 for this demo\n");
                } else {
                    fib = new MemoizedFibonacci(gridConfig);
                    fn = fib.fibonacci(n);
                    System.out.printf("fibonacci(%d) = %d", n, fn);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

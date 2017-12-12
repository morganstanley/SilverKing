package com.ms.silverking.cloud.dht.client.example;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

import com.ms.silverking.cloud.dht.client.AsyncOperation;
import com.ms.silverking.cloud.dht.client.AsyncOperationListener;
import com.ms.silverking.cloud.dht.client.AsyncPut;
import com.ms.silverking.cloud.dht.client.AsyncValueRetrieval;
import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.OperationState;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;

public class HelloCallbackDHT2 implements AsyncOperationListener {    
	private Semaphore	s;
	
	private static final int	numValues = 1000;
	
	public String runExample(SKGridConfiguration gridConfig) {
		try {
            AsynchronousNamespacePerspective<String, String>    asyncNSP;
            AsyncPut<String>    asyncPut;
            AsyncValueRetrieval<String,String>    asyncGet;
            Map<String,String>	v1;

            s = new Semaphore(0);
            asyncNSP = new DHTClient().openSession(gridConfig)
                    .openAsyncNamespacePerspective("_MyNamespace" + System.currentTimeMillis(), String.class, String.class);
            v1 = new HashMap<>();
            for (int i = 0; i < numValues; i++) {
            	v1.put(Integer.toString(i), Integer.toString(i));
            }
            asyncPut = asyncNSP.put(v1);
            asyncPut.addListener(this, OperationState.SUCCEEDED, OperationState.FAILED, OperationState.INCOMPLETE);
            s.acquire();
            s.drainPermits();
            asyncGet = asyncNSP.get(v1.keySet());
            asyncGet.addListener(this, OperationState.SUCCEEDED, OperationState.FAILED, OperationState.INCOMPLETE);
            s.acquire();
            return "complete";//asyncGet.getValues().toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
	}
	
	@Override
	public void asyncOperationUpdated(AsyncOperation asyncOperation) {
		OperationState	opState;
		
		opState = asyncOperation.getState();
		if (opState == OperationState.FAILED) {
			System.out.printf("Operation failed: %s\n", asyncOperation.getClass());
		} else if (opState == OperationState.INCOMPLETE || opState == OperationState.SUCCEEDED) {
			if (asyncOperation instanceof AsyncValueRetrieval) {
				AsyncValueRetrieval	asyncValueRetrieval;
				Map<String,String>	values;
				
				asyncValueRetrieval = (AsyncValueRetrieval)asyncOperation;
				try {
					values = asyncValueRetrieval.getLatestValues();
					for (Map.Entry<String,String> entry : values.entrySet()) {
						System.out.printf("Complete: %s\t=>\t%s\n", entry.getKey(), entry.getValue());
					}
				} catch (RetrievalException e) {
					e.printStackTrace();
				}
				//System.out.printf("Incomplete keys: %d\n", asyncValueRetrieval.getIncompleteKeys().size());
			} else {
				System.out.printf("Operation incomplete: %s\n", asyncOperation.getClass());
			}
			if (opState == OperationState.SUCCEEDED) {
				s.release();
			}
		} else {
			s.release();
		}
		//System.out.printf("%s %s %s\n", Thread.currentThread().getName(), asyncOperation.getClass(), opState);
	}
	
    public static void main(String[] args) throws IOException {
    	System.out.println( new HelloCallbackDHT2().runExample( SKGridConfiguration.parseFile(args[0]) ) );
    }
}

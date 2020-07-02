package com.ms.silverking.cloud.dht.client.example;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;

import com.ms.silverking.cloud.dht.client.AsyncOperation;
import com.ms.silverking.cloud.dht.client.AsyncOperationListener;
import com.ms.silverking.cloud.dht.client.AsyncPut;
import com.ms.silverking.cloud.dht.client.AsyncSingleValueRetrieval;
import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.OperationState;
import com.ms.silverking.cloud.dht.client.SimpleTimeoutController;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.log.Log;
import com.ms.silverking.thread.ThreadUtil;

public class HelloCallbackDHT implements AsyncOperationListener {
  private Semaphore s;

  public String runExample(SKGridConfiguration gridConfig) {
    try {
      AsynchronousNamespacePerspective<String, String> asyncNSP;
      AsyncPut<String> asyncPut;
      AsyncSingleValueRetrieval<String, String> asyncGet;
      DHTSession  session;
      Namespace ns;

      s = new Semaphore(0);
      session = new DHTClient().openSession(gridConfig);
      ns = session.createNamespace("_MyNamespace" + System.currentTimeMillis());
      asyncNSP = ns.openAsyncPerspective(String.class, String.class);
      asyncPut = asyncNSP.put("Hello callback", "callback world!");
      asyncPut.addListener(this, OperationState.SUCCEEDED, OperationState.FAILED, OperationState.INCOMPLETE);
      s.acquire();
      s.drainPermits();
      
      System.out.println("Sleeping...");
      ThreadUtil.sleep(20 * 1000);
      System.out.println("Awake");
      //Log.setLevel(Level.ALL);
      
      //asyncGet = asyncNSP.get("Hello callback");
      System.out.println(asyncNSP.getOptions().getDefaultGetOptions());
      asyncGet = asyncNSP.get("Hello callback");
      //asyncGet = asyncNSP.get("Hello callback", asyncNSP.getOptions().getDefaultGetOptions().opTimeoutController(new SimpleTimeoutController(1, 1000)));
      asyncGet.addListener(this);
      s.acquire();
      return asyncGet.getValue();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void asyncOperationUpdated(AsyncOperation asyncOperation) {
    if (asyncOperation.getState() == OperationState.FAILED) {
      System.out.printf("Operation failed: %s\n", asyncOperation);
      s.release();
    } else if (asyncOperation.getState() == OperationState.INCOMPLETE) {
      System.out.printf("Operation incomplete: %s\n", asyncOperation);
    } else {
      s.release();
    }
    System.out.printf("%s\n", Thread.currentThread().getName());
  }

  public static void main(String[] args) throws IOException {
    System.out.println(new HelloCallbackDHT().runExample(SKGridConfiguration.parseFile(args[0])));
  }
}

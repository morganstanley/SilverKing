package com.ms.silverking.cloud.dht.client.example;

import java.io.IOException;

import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.client.AsyncPut;
import com.ms.silverking.cloud.dht.client.AsyncSingleValueRetrieval;
import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;

public class HelloAsyncMetaData {

  public static String runExample(SKGridConfiguration gridConfig) {
    try {
      AsynchronousNamespacePerspective<String, String> asyncNSP;
      AsyncPut<String> asyncPut;
      AsyncSingleValueRetrieval<String, String> asyncRetrieval;
      Namespace ns;
      String  nsName;
      DHTSession  session;
      
      nsName = "_MyNamespace." + System.currentTimeMillis();
      session = new DHTClient().openSession(gridConfig);
      ns = session.createNamespace(nsName, session.getDefaultNamespaceOptions().versionMode(NamespaceVersionMode.SYSTEM_TIME_MILLIS));
      asyncNSP = ns.openAsyncPerspective(String.class, String.class);
      asyncPut = asyncNSP.put("Hello async", "async world!");
      asyncPut.waitForCompletion();
      System.out.printf("Stored version: %d\n", asyncPut.getStoredVersion());
      asyncRetrieval = asyncNSP.get("Hello async", asyncNSP.getOptions().getDefaultGetOptions().retrievalType(RetrievalType.VALUE_AND_META_DATA));
      asyncRetrieval.waitForCompletion();
      System.out.printf("Retreived metadata: %s\n", asyncRetrieval.getStoredValue().getMetaData().toString(true));
      return asyncRetrieval.getStoredValue().getValue();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws IOException {
    System.out.println(runExample(SKGridConfiguration.parseFile(args[0])));
  }
}

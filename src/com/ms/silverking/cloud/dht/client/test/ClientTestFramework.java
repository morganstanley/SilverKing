package com.ms.silverking.cloud.dht.client.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.client.ClientDHTConfigurationProvider;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.EmbeddedSK;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.NamespaceCreationException;
import com.ms.silverking.cloud.dht.daemon.storage.NamespaceNotCreatedException;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;

public class ClientTestFramework {
  private final DHTClient dhtClient;
  private final DHTSession session;

  private static final String nsNameBase = "Test.";

  public ClientTestFramework(String gcName) throws IOException, ClientException {
    ClientDHTConfigurationProvider config;
    Namespace ns;

    if (gcName == null) {
      Log.warning("Creating embedded SK instance");
      config = EmbeddedSK.createEmbeddedSKInstance();
      Log.warning("Embedded SK instance running at: " + config);
    } else {
      Log.warningf("Using SK instance %s\n", gcName);
      config = SKGridConfiguration.parseFile(gcName);
    }

    dhtClient = new DHTClient();
    Log.warning("Opening session");
    session = new DHTClient().openSession(config);
  }

  public List<Triple<String, Integer, Integer>> runTests(List<ClientTest> tests) {
    List<Triple<String, Integer, Integer>> results;

    results = new ArrayList<>();
    for (ClientTest test : tests) {
      results.addAll(runTest(test));
    }
    return ImmutableList.copyOf(results);
  }

  public List<Triple<String, Integer, Integer>> runTest(ClientTest test) {
    List<Triple<String, Integer, Integer>> results;

    results = new ArrayList<>();
    for (NamespaceOptions nsOptions : test.getNamespaceOptions(session.getDefaultNamespaceOptions())) {
      results.add(runTest(test, nsOptions));
    }
    return results;
  }

  public Triple<String, Integer, Integer> runTest(ClientTest test, NamespaceOptions nsOptions) {
    try {
      Namespace ns;
      UUIDBase id;

      id = UUIDBase.random();
      ns = createNamespace(test.getTestName() + id.toString(), nsOptions);
      Log.warningf("\nRunning test: %s\n", test.getTestName());
      Log.warningf("nsOptions %s", nsOptions);
      return Triple.of(test.getTestName(), test.runTest(session, ns));
    } catch (NamespaceCreationException nce) {
      Log.logErrorWarning(nce, "Unable to create namespace for test " + test.getTestName());
      return Triple.of(test.getTestName(), 0, 0);
    }
  }

  private Namespace createNamespace(String nsName, NamespaceOptions nsOptions) throws NamespaceCreationException {
    Namespace ns;

    try {
      ns = session.getNamespace(nsName);
    } catch (NamespaceNotCreatedException ne) {
      ns = null;
    }
    if (ns == null) {
      Log.warningf("Creating namespace %s\n", nsName);
      ns = session.createNamespace(nsName, nsOptions);
    } else {
      Log.warningf("Found namespace %s\n", nsName);
    }
    return ns;
  }

  public static void main(String[] args) {
    try {
      if (args.length > 2) {
        System.out.println("args: <gcName> [test,...]");
        System.out.println("args: none");
      } else {
        ClientTestFramework tf;
        String gcName;
        List<ClientTest> tests;
        List<Triple<String, Integer, Integer>> results;

        if (args.length == 0) {
          gcName = null;
          tests = ClientTestRegistry.getAllTests();
        } else {
          gcName = args[0];
          if (args.length == 1) {
            tests = ClientTestRegistry.getAllTests();
          } else {
            tests = ClientTestRegistry.getTests(CollectionUtil.parseSet(args[1], ","));
          }
        }
        tf = new ClientTestFramework(gcName);
        results = tf.runTests(tests);
        Log.warning("\n** Test results **");
        for (Triple<String, Integer, Integer> result : results) {
          Log.warningf("%s\t%d\t%d\n", result.getV1(), result.getV2(), result.getV3());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

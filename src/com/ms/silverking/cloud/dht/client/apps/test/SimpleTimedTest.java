package com.ms.silverking.cloud.dht.client.apps.test;

import java.io.PrintStream;

import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

public class SimpleTimedTest {
  private final SKGridConfiguration gridConfig;
  private final PrintStream out;
  private final PrintStream err;

  private static final String nsBase = "SimpleTimedTest.";

  public SimpleTimedTest(SKGridConfiguration gridConfig, PrintStream out, PrintStream err) {
    this.gridConfig = gridConfig;
    this.out = out;
    this.err = err;
  }

  public void runTest(String _ns) throws Exception {
    DHTSession session;
    Namespace ns;
    String nsName;
    Stopwatch sessionSW;
    Stopwatch nsSW;
    Stopwatch nsSW2;
    Stopwatch nsSW3;

    if (_ns != null) {
      nsName = _ns;
    } else {
      nsName = nsBase + System.currentTimeMillis();
    }
    System.out.println("1*************************************************1");
    sessionSW = new SimpleStopwatch();
    session = new DHTClient().openSession(gridConfig);
    sessionSW.stop();
    System.out.println("2*************************************************2");
    nsSW = new SimpleStopwatch();
    if (_ns == null) {
      ns = session.createNamespace(nsName);
    } else {
      ns = session.getNamespace(nsName);
    }
    nsSW.stop();
    System.out.println("3*************************************************3");
    nsSW2 = new SimpleStopwatch();
    if (_ns == null) {
      ns = session.createNamespace(nsName + "2");
    } else {
      ns = session.getNamespace(nsName + "2");
    }
    nsSW2.stop();
    System.out.println("4*************************************************4");
    nsSW3 = new SimpleStopwatch();
    if (_ns == null) {
      ns = session.createNamespace(nsName + "3");
    } else {
      ns = session.getNamespace(nsName + "3");
    }
    nsSW3.stop();
    System.out.println("5*************************************************5");

    System.out.printf("Session creation time:    %f\n", sessionSW.getElapsedSeconds());
    System.out.printf("Namespace creation time:  %f\n", nsSW.getElapsedSeconds());
    System.out.printf("Namespace creation time2: %f\n", nsSW2.getElapsedSeconds());
    System.out.printf("Namespace creation time3: %f\n", nsSW3.getElapsedSeconds());
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    Stopwatch sw;

    sw = new SimpleStopwatch();
    try {
      if (args.length != 1 && args.length != 2) {
        System.err.println("Usage: <gridConfig> [ns]");
      } else {
        SimpleTimedTest test;
        String gridConfig;
        String ns;

        gridConfig = args[0];
        if (args.length == 2) {
          ns = args[1];
        } else {
          ns = null;
        }
        test = new SimpleTimedTest(SKGridConfiguration.parseFile(gridConfig), System.out, System.err);
        test.runTest(ns);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    sw.stop();
    System.out.printf("Total time:               %f\n", sw.getElapsedSeconds());
  }
}

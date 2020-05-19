package com.ms.silverking.cloud.dht.client.test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.log.Log;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

public class ParameterizedNamespaceTest implements ClientTest {
  private final NamespaceOptions nsOptions;

  private static final int minBatchSize = 1;
  private static final int maxBatchSize = 131072;
  private static final int writeBaseTime = 20 * 1000;
  private static final int writePerItemTime = 200;
  private static final int readBaseTime = 20 * 1000;
  private static final int readPerItemTime = 200;
  private static final int minValueSize = 32;
  private static final int maxValueSize = 8192;
  private static final int shiftSize = 2;

  public ParameterizedNamespaceTest(NamespaceOptions nsOptions) {
    this.nsOptions = nsOptions;
  }

  @Override
  public String getTestName() {
    return nsOptions.toString();
  }

  @Override
  public List<NamespaceOptions> getNamespaceOptions(NamespaceOptions sessionDefaultNSOptions) {
    return ImmutableList.of(nsOptions);
  }

  @Override
  public Pair<Integer, Integer> runTest(DHTSession session, Namespace ns) {
    SynchronousNamespacePerspective<String, String> syncNSP;

    syncNSP = ns.openSyncPerspective(ns.getDefaultNSPOptions(String.class, String.class));
    return test(syncNSP);
  }

  public Pair<Integer, Integer> test(SynchronousNamespacePerspective<String, String> syncNSP) {
    Pair<Integer, Integer> writeResults;
    Pair<Integer, Integer> readResults;

    writeResults = testWrite(syncNSP);
    readResults = testRead(syncNSP);
    return new Pair<>(writeResults.getV1() + readResults.getV1(), writeResults.getV2() + readResults.getV2());
  }

  public Pair<Integer, Integer> testWrite(SynchronousNamespacePerspective<String, String> syncNSP) {
    int successful;
    int failed;

    successful = 0;
    failed = 0;
    for (int i = minBatchSize; i <= maxBatchSize; i = (i << shiftSize)) {
      System.out.printf("Writing %d...", i);
      try {
        Stopwatch sw;

        sw = new SimpleStopwatch();
        if (nsOptions.getVersionMode() == NamespaceVersionMode.CLIENT_SPECIFIED) {
          PutOptions po;

          po = syncNSP.getOptions().getDefaultPutOptions().version(SystemTimeUtil.skSystemTimeSource.absTimeNanos());
          syncNSP.put(createBatchForWrite(i), po);
        } else {
          syncNSP.put(createBatchForWrite(i));
        }
        sw.stop();
        if (sw.getElapsedMillis() <= writeTimeLimit(i)) {
          ++successful;
          System.out.printf("ok %f\n", sw.getElapsedSeconds());
        } else {
          ++failed;
          System.out.printf("failed due to time exceeded %f\n", sw.getElapsedSeconds());
        }
      } catch (Exception e) {
        ++failed;
        System.out.printf("failed due to exception\n");
        Log.logErrorWarning(e);
        if (e instanceof PutException) {
          PutException pe;

          pe = (PutException) e;
          System.out.println(pe.getDetailedFailureMessage());
        }
      }
    }
    return new Pair<>(successful, failed);
  }

  public Pair<Integer, Integer> testRead(SynchronousNamespacePerspective<String, String> syncNSP) {
    int successful;
    int failed;

    successful = 0;
    failed = 0;
    for (int i = minBatchSize; i <= maxBatchSize; i = (i << shiftSize)) {
      System.out.printf("Reading %d...", i);
      try {
        Map<String, String> batch;
        Stopwatch sw;

        sw = new SimpleStopwatch();
        batch = syncNSP.get(createBatchForRead(i));
        sw.stop();
        if (sw.getElapsedMillis() <= writeTimeLimit(i)) {
          if (checkBatch(batch, i)) {
            ++successful;
            System.out.printf("ok %f\n", sw.getElapsedSeconds());
          } else {
            ++failed;
            System.out.printf("verify failed\n");
          }
        } else {
          ++failed;
          System.out.printf("failed due to time exceeded %f\n", sw.getElapsedSeconds());
        }
      } catch (Exception e) {
        ++failed;
        System.out.printf("failed due to exception\n");
        Log.logErrorWarning(e);
      }
    }
    return new Pair<>(successful, failed);
  }

  private boolean checkBatch(Map<String, String> batch, int batchSize) {
    boolean ok;

    ok = true;
    for (int i = 0; i < batchSize; i++) {
      String v;

      v = batch.get(Integer.toString(i));
      if (v != null) {
        String check;
        int index;

        index = v.indexOf('\0');
        if (index < 0) {
          ok = false;
          System.out.printf("Can't parse value for %d; found %s\n", i, v);
        } else {
          check = v.substring(0, index);
          if (check.equals(Integer.toString(i))) {
            // ok
          } else {
            ok = false;
            System.out.printf("Value mismatch for %d; found %s\n", i, v);
          }
        }
      } else {
        ok = false;
        System.out.printf("Can't find %d\n", i);
      }
    }
    return ok;
  }

  private Map<String, String> createBatchForWrite(int batchSize) {
    Map<String, String> batch;

    batch = new HashMap<>();
    for (int i = 0; i < batchSize; i++) {
      byte[] value;
      byte[] id;

      id = (Integer.toString(i) + "\0").getBytes();
      value = new byte[ThreadLocalRandom.current().nextInt(minValueSize, maxValueSize + 1)];
      if (nsOptions.getVersionMode() != NamespaceVersionMode.SINGLE_VERSION) {
        ThreadLocalRandom.current().nextBytes(value);
      }
      System.arraycopy(id, 0, value, 0, id.length);
      batch.put(Integer.toString(i), new String(value));
    }
    return batch;
  }

  private Set<String> createBatchForRead(int batchSize) {
    Set<String> keys;

    keys = new HashSet<>();
    for (int i = 0; i < batchSize; i++) {
      keys.add(Integer.toString(i));
    }
    return keys;
  }

  private int writeTimeLimit(int batchSize) {
    return writeBaseTime + writePerItemTime * batchSize;
  }

  private int readTimeLimit(int batchSize) {
    return readBaseTime + readPerItemTime * batchSize;
  }
}

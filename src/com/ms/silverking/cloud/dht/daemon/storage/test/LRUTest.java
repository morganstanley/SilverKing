package com.ms.silverking.cloud.dht.daemon.storage.test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.LRURetentionPolicy;
import com.ms.silverking.cloud.dht.LRWRetentionPolicy;
import com.ms.silverking.cloud.dht.NamespaceServerSideCode;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.ValueRetentionPolicy;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.NamespaceCreationException;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.StoredValue;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.daemon.storage.NamespaceNotCreatedException;
import com.ms.silverking.cloud.dht.daemon.storage.serverside.LRUTrigger;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.time.SimpleTimer;
import com.ms.silverking.time.Timer;

/**
 * Simple test of LRU value retention.
 */
public class LRUTest {
  private final DHTClient dhtClient;
  private final DHTSession dhtSession;

  private static final int maxValueSize = 20_000_000;
  private static byte[] randomBuf = new byte[maxValueSize];

  private static final NamespaceServerSideCode nsServerSideCode = NamespaceServerSideCode.singleTrigger(
      LRUTrigger.class);

  private static final int displayIntervalSeconds = 1;

  static {
    ThreadLocalRandom.current().nextBytes(randomBuf);
  }

  public LRUTest(SKGridConfiguration gc) throws IOException, ClientException {
    System.out.printf("Creating DHTClient\n");
    dhtClient = new DHTClient();
    System.out.printf("Opening DHTSession\n");
    dhtSession = dhtClient.openSession(gc);
  }

  /**
   * Run the LRUTest with the given parameters
   *
   * @param namespace
   * @param runtimeSeconds
   * @param vrp
   * @param minValueSize
   * @param maxValueSize
   * @throws NamespaceCreationException
   * @throws PutException
   * @throws RetrievalException
   */
  public void runTest(String namespace, int runtimeSeconds, ValueRetentionPolicy vrp, int minValueSize, int maxValueSize,
      int storageFormat)
      throws NamespaceCreationException, PutException, RetrievalException {
    Namespace ns;
    SynchronousNamespacePerspective<String, byte[]> syncNSP;

    System.out.printf("ValueRetentionPolicy: %s\n", vrp);
    System.out.printf("Checking for namespace %s\n", namespace);
    try {
      ns = dhtSession.getNamespace(namespace);
    } catch (NamespaceNotCreatedException nnce) {
      ns = null;
    }
    if (ns != null) {
      throw new RuntimeException("Namespace already exists");
    } else {
      int numValuesWritten;
      int numValuesFound;
      Pair<Integer, Integer>  writeResult;
      Triple<Integer, Long, Long> checkResult;

      System.out.printf("Creating namespace %s\n", namespace);
      ns = dhtSession.createNamespace(namespace, dhtSession.getDefaultNamespaceOptions().consistencyProtocol(
          ConsistencyProtocol.LOOSE).namespaceServerSideCode(nsServerSideCode).valueRetentionPolicy(vrp).versionMode(
          NamespaceVersionMode.SYSTEM_TIME_NANOS).storageFormat(Integer.toString(storageFormat)));
      System.out.printf("NamespaceOptions:\n%s\n", ns.getOptions());
      syncNSP = ns.openSyncPerspective(String.class, byte[].class);

      System.out.printf("Writing values\n");
      writeResult = writeValues(syncNSP, runtimeSeconds, minValueSize, maxValueSize);
      numValuesWritten = writeResult.getV2();
      System.out.printf("numValuesWritten: %d\n", numValuesWritten);
      System.out.printf("warming valuesFound: %d\n", writeResult.getV1());

      System.out.printf("Checking values\n");
      checkResult = checkValues(syncNSP, numValuesWritten);
      numValuesFound = checkResult.getV1();
      System.out.printf("numValuesFound:   %d\n", numValuesFound);
      System.out.printf("compressedBytesFound:     %d\n", checkResult.getV2());
      System.out.printf("uncompressedBytesFound:   %d\n", checkResult.getV3());

      System.out.printf("Complete\n");
    }
  }

  private Pair<Integer, Integer> writeValues(SynchronousNamespacePerspective<String, byte[]> syncNSP, int runtimeSeconds, int minValueSize,
      int maxValueSize) throws PutException, RetrievalException {
    Timer testTimer;
    int key;
    Timer displayTimer;
    int randomValuesFound;

    randomValuesFound = 0;
    testTimer = new SimpleTimer(TimeUnit.SECONDS, runtimeSeconds);
    displayTimer = new SimpleTimer(TimeUnit.SECONDS, displayIntervalSeconds);
    key = 0;
    while (!testTimer.hasExpired()) {
      int size;
      byte[] value;

      size = ThreadLocalRandom.current().nextInt(minValueSize, maxValueSize + 1);
      value = new byte[size];
      System.arraycopy(randomBuf, 0, value, 0, value.length);
      if (displayTimer.hasExpired()) {
        System.out.printf("Written: %d\n", key);
        displayTimer.reset();
      }
      syncNSP.put(key(key), value);
      randomValuesFound += readRandomValue(syncNSP, key);
      randomValuesFound += readSomeValues(syncNSP, key);
      ++key;
    }
    return new Pair<>(randomValuesFound, key);
  }

  private int readSomeValues(SynchronousNamespacePerspective<String, byte[]> syncNSP, int maxKey)
      throws RetrievalException {
    Set<String> keys;
    Map<String,byte[]> values;
    int found;

    keys = new HashSet<>();
    for (int i = 0; i < maxKey; i += 1000) {
      keys.add(key(i));
    }
    values = syncNSP.get(keys);
    found = 0;
    for (String key : keys) {
      if (values.get(key) != null) {
        found++;
      }
    }
    return found;
  }

  private int readRandomValue(SynchronousNamespacePerspective<String, byte[]> syncNSP, int maxKey)
      throws RetrievalException {
    if (maxKey > 0) {
      int key;

      key = ThreadLocalRandom.current().nextInt(maxKey);
      if (syncNSP.get(key(key)) != null) {
        return 1;
      } else {
        return 0;
      }
    } else {
      return 0;
    }
  }

  private Triple<Integer, Long, Long> checkValues(SynchronousNamespacePerspective<String, byte[]> syncNSP, int numValuesWritten)
      throws RetrievalException {
    int numValuesFound;
    long  compressedBytesFound;
    long  uncompressedBytesFound;

    numValuesFound = 0;
    compressedBytesFound = 0;
    uncompressedBytesFound = 0;
    for (int i = numValuesWritten - 1; i >= 0; i--) { // backwards to avoid purging old data; touch new instead
                                                      // as the new will be in the head segment
      StoredValue sv;

      sv = syncNSP.retrieve(key(i), syncNSP.getOptions().getDefaultGetOptions().retrievalType(RetrievalType.META_DATA));
      if (sv != null) {
        numValuesFound++;
        compressedBytesFound += sv.getStoredLength();
        uncompressedBytesFound += sv.getUncompressedLength();
      }
    }
    return Triple.of(numValuesFound, compressedBytesFound, uncompressedBytesFound);
  }

  private String key(int i) {
    return "key." + i;
  }

  public static void main(String[] args) {
    if (args.length != 8) {
      System.out.println(
          "args: <gridConfig> <namespace> <runtimeSeconds> <valueRetentionPolicy [lru|lrw]> <bytesToRetain> <minValueSize> <maxValueSize> <storageFormat>");
    } else {
      try {
        LRUTest lruTest;
        SKGridConfiguration gc;
        String namespace;
        int runtimeSeconds;
        long bytesToRetain;
        int minValueSize;
        int maxValueSize;
        int storageFormat;
        ValueRetentionPolicy vrp;

        gc = SKGridConfiguration.parseFile(args[0]);
        namespace = args[1];
        runtimeSeconds = Integer.parseInt(args[2]);
        bytesToRetain = Long.parseLong(args[4]);
        minValueSize = Integer.parseInt(args[5]);
        maxValueSize = Integer.parseInt(args[6]);
        storageFormat = Integer.parseInt(args[7]);

        if (args[3].equalsIgnoreCase("lrw")) {
          vrp = new LRWRetentionPolicy(bytesToRetain);
        } else {
          vrp = new LRURetentionPolicy(bytesToRetain, LRURetentionPolicy.DO_NOT_PERSIST);
        }

        lruTest = new LRUTest(gc);
        lruTest.runTest(namespace, runtimeSeconds, vrp, minValueSize, maxValueSize, storageFormat);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}

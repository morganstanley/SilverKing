package com.ms.silverking.cloud.dht.daemon.storage;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.log.Log;
import com.ms.silverking.text.StringUtil;

public class NamespaceMetrics {
  private volatile int totalKeys; // meta write lock is held
  private AtomicLong bytesUncompressed;
  private AtomicLong bytesCompressed;
  private volatile long totalPuts; // write lock is held when updating puts
  private volatile long totalInvalidations; // write lock is held when updating puts
  private AtomicLong totalRetrievals; // only read lock is held; use atomic
  private volatile long lastPutMillis;
  private volatile long lastRetrievalMillis;

  private static final Set<String> metrics;

  static {
    Set<String> _metrics;

    _metrics = new HashSet<>();
    for (Field field : NamespaceMetrics.class.getDeclaredFields()) {
      if (!Modifier.isStatic(field.getModifiers())) {
        Log.finef("%s\n", field.getName());
        _metrics.add(field.getName());
      }
    }
    metrics = ImmutableSet.copyOf(_metrics);
  }

  public static NamespaceMetrics aggregate(NamespaceMetrics nm0, NamespaceMetrics nm1) {
    NamespaceMetrics a;

    a = new NamespaceMetrics();
    a.totalKeys = nm0.totalKeys + nm1.totalKeys;
    a.bytesUncompressed.set(nm0.bytesUncompressed.longValue() + nm1.bytesUncompressed.longValue());
    a.bytesCompressed.set(nm0.bytesCompressed.longValue() + nm1.bytesCompressed.longValue());
    a.totalPuts = nm0.totalPuts + nm1.totalPuts;
    a.totalInvalidations = nm0.totalInvalidations + nm1.totalInvalidations;
    a.totalRetrievals.set(nm0.totalRetrievals.longValue() + nm1.totalRetrievals.longValue());
    a.lastPutMillis = Math.max(nm0.lastPutMillis, nm1.lastPutMillis);
    a.lastRetrievalMillis = Math.max(nm0.lastRetrievalMillis, nm1.lastRetrievalMillis);
    return a;
  }

  public NamespaceMetrics() {
    bytesUncompressed = new AtomicLong();
    bytesCompressed = new AtomicLong();
    totalRetrievals = new AtomicLong();
  }

  public static Set<String> getMetricNames() {
    return metrics;
  }

  public String getMetric(String name) {
    if (!metrics.contains(name)) {
      return null;
    } else {
      return _getMetric(name);
    }
  }

  private String _getMetric(String name) {
    try {
      Method method;
      Object result;

      method = NamespaceMetrics.class.getMethod("get" + StringUtil.firstCharToUpperCase(name));
      result = method.invoke(this, new Object[0]);
      return result.toString();
    } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      Log.logErrorWarning(e, "getMetric failed: " + name);
      return null;
    }
  }

  public int getTotalKeys() {
    return totalKeys;
  }

  public long getBytesUncompressed() {
    return bytesUncompressed.get();
  }

  public long getBytesCompressed() {
    return bytesCompressed.get();
  }

  public void incTotalKeys() {
    ++totalKeys;
  }

  public void addBytes(int bytesUncompressed, int bytesCompressed) {
    this.bytesUncompressed.addAndGet(bytesUncompressed);
    this.bytesCompressed.addAndGet(bytesCompressed);
  }

  public void addPuts(int numPuts, int numInvalidations, long timeMillis) {
    totalPuts += numPuts;
    totalInvalidations += numInvalidations;
    lastPutMillis = timeMillis;
  }

  public void addRetrievals(int numRetrievals, long timeMillis) {
    totalRetrievals.addAndGet(numRetrievals);
    lastRetrievalMillis = timeMillis;
  }

  public long getTotalPuts() {
    return totalPuts;
  }

  public long getTotalInvalidations() {
    return totalInvalidations;
  }

  public long getTotalRetrievals() {
    return totalRetrievals.get();
  }

  public long getLastPutMillis() {
    return lastPutMillis;
  }

  public long getLastRetrievalMillis() {
    return lastRetrievalMillis;
  }

  public long getLastActivityMillis() {
    return Math.max(getLastPutMillis(), getLastRetrievalMillis());
  }

  public static void main(String[] args) {
    System.out.printf("%s\n", new NamespaceMetrics().getMetric("totalRetrievals"));
  }
}
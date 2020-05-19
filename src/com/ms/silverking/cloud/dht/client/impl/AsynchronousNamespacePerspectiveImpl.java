package com.ms.silverking.cloud.dht.client.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.GetOptions;
import com.ms.silverking.cloud.dht.InvalidationOptions;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.WaitOptions;
import com.ms.silverking.cloud.dht.client.AsyncInvalidation;
import com.ms.silverking.cloud.dht.client.AsyncOperation;
import com.ms.silverking.cloud.dht.client.AsyncPut;
import com.ms.silverking.cloud.dht.client.AsyncRetrieval;
import com.ms.silverking.cloud.dht.client.AsyncSingleRetrieval;
import com.ms.silverking.cloud.dht.client.AsyncSingleValueRetrieval;
import com.ms.silverking.cloud.dht.client.AsyncSyncRequest;
import com.ms.silverking.cloud.dht.client.AsyncValueRetrieval;
import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.OperationException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.WaitForCompletionException;
import com.ms.silverking.cloud.dht.client.impl.ClientNamespace.OpLWTMode;

class AsynchronousNamespacePerspectiveImpl<K, V> extends BaseNamespacePerspectiveImpl<K, V>
    implements AsynchronousNamespacePerspective<K, V> {

  private static final OpLWTMode opLWTMode;

  static {
    opLWTMode = OpLWTMode.DisallowUserThreadUsage;
  }

  AsynchronousNamespacePerspectiveImpl(ClientNamespace clientNamespace, String name,
      NamespacePerspectiveOptionsImpl<K, V> nspoImpl) {
    super(clientNamespace, name, nspoImpl);
  }

  // reads

  @Override
  public AsyncRetrieval<K, V> retrieve(Set<? extends K> keys, RetrievalOptions retrievalOptions)
      throws RetrievalException {
    return baseRetrieve(keys, retrievalOptions, opLWTMode);
  }

  @Override
  public AsyncRetrieval<K, V> retrieve(Set<? extends K> keys) throws RetrievalException {
    return retrieve(keys, nspoImpl.getDefaultGetOptions());
  }

  @Override
  public AsyncSingleRetrieval<K, V> retrieve(K key, RetrievalOptions retrievalOptions) throws RetrievalException {
    return (AsyncSingleRetrieval<K, V>) baseRetrieve(ImmutableSet.of(key), retrievalOptions, opLWTMode);
  }

  @Override
  public AsyncSingleRetrieval<K, V> retrieve(K key) throws RetrievalException {
    return retrieve(key, nspoImpl.getDefaultGetOptions());
  }

  // gets

  @Override
  public AsyncValueRetrieval<K, V> get(Set<? extends K> keys, GetOptions getOptions) throws RetrievalException {
    return (AsyncValueRetrieval<K, V>) retrieve(keys, getOptions);
  }

  @Override
  public AsyncValueRetrieval<K, V> get(Set<? extends K> keys) throws RetrievalException {
    return get(keys, nspoImpl.getDefaultGetOptions());
  }

  @Override
  public AsyncSingleValueRetrieval<K, V> get(K key, GetOptions getOptions) throws RetrievalException {
    return (AsyncSingleValueRetrieval<K, V>) retrieve(ImmutableSet.of(key), getOptions);
  }

  @Override
  public AsyncSingleValueRetrieval<K, V> get(K key) throws RetrievalException {
    return get(key, nspoImpl.getDefaultGetOptions());
  }

  // waitfors

  @Override
  public AsyncValueRetrieval<K, V> waitFor(Set<? extends K> keys, WaitOptions waitOptions) throws RetrievalException {
    return (AsyncValueRetrieval<K, V>) retrieve(keys, waitOptions);
  }

  @Override
  public AsyncValueRetrieval<K, V> waitFor(Set<? extends K> keys) throws RetrievalException {
    return waitFor(keys, nspoImpl.getDefaultWaitOptions());
  }

  @Override
  public AsyncSingleValueRetrieval<K, V> waitFor(K key, WaitOptions waitOptions) throws RetrievalException {
    return (AsyncSingleValueRetrieval<K, V>) retrieve(ImmutableSet.of(key), waitOptions);
  }

  @Override
  public AsyncSingleValueRetrieval<K, V> waitFor(K key) throws RetrievalException {
    return waitFor(key, nspoImpl.getDefaultWaitOptions());
  }

  // puts

  @Override
  public AsyncPut<K> put(Map<? extends K, ? extends V> values, PutOptions putOptions) {
    return basePut(values, putOptions, this, opLWTMode);
  }

  @Override
  public AsyncPut<K> put(Map<? extends K, ? extends V> values) {
    return put(values, nspoImpl.getDefaultPutOptions());
  }

  @Override
  public AsyncPut<K> put(K key, V value, PutOptions putOptions) {
    return put(ImmutableMap.of(key, value), putOptions);
  }

  @Override
  public AsyncPut<K> put(K key, V value) {
    return put(key, value, nspoImpl.getDefaultPutOptions());
  }

  // invalidations

  public AsyncInvalidation<K> invalidate(Set<? extends K> keys, InvalidationOptions invalidationOptions) {
    return baseInvalidation(keys, invalidationOptions, nspoImpl.getValueSerializer(), opLWTMode);
  }

  public AsyncInvalidation<K> invalidate(Set<? extends K> keys) {
    return invalidate(keys, nspoImpl.getDefaultInvalidationOptions());
  }

  public AsyncInvalidation<K> invalidate(K key, InvalidationOptions invalidationOptions) {
    return invalidate(ImmutableSet.of(key), invalidationOptions);
  }

  public AsyncInvalidation<K> invalidate(K key) {
    return invalidate(key, nspoImpl.getDefaultInvalidationOptions());
  }

  //

  @Override
  public void waitForActiveOps() throws WaitForCompletionException {
    List<AsyncOperationImpl> activeAsyncOps;
    List<AsyncOperation> failedOps;

    failedOps = new ArrayList<>();
    activeAsyncOps = clientNamespace.getActiveAsyncOperations();
    for (AsyncOperationImpl activeAsyncOp : activeAsyncOps) {
      try {
        activeAsyncOp.waitForCompletion();
      } catch (OperationException oe) {
        failedOps.add(activeAsyncOp);
      }
    }
    if (failedOps.size() > 0) {
      throw new WaitForCompletionException(failedOps);
    }
  }

  public AsyncSyncRequest syncRequest(long version) {
    return super.baseSyncRequest(version, this);
  }

  public AsyncSyncRequest syncRequest() {
    return syncRequest(getAbsMillisTimeSource().absTimeMillis());
  }
}

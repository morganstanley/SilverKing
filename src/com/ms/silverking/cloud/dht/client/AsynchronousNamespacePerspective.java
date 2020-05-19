package com.ms.silverking.cloud.dht.client;

/**
 * Read/write interface to the DHT. Asynchronous - all methods will return quickly
 * to the caller after returning some descendant of AsyncOperation.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface AsynchronousNamespacePerspective<K, V>
    extends AsynchronousWritableNamespacePerspective<K, V>, AsynchronousReadableNamespacePerspective<K, V> {
  /**
   * Wait for all all active asynchronous calls to complete.
   * Failure of any operations will result in an WaitForCompletionException
   * which will contain a list of all failed operations.
   *
   * @throws WaitForCompletionException TODO
   */
  public void waitForActiveOps() throws WaitForCompletionException;
}

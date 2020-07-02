package com.ms.silverking.cloud.dht.daemon.storage.management;

/**
 * A wrapper for NamespaceNotCreatedException to force caller to handle NamespaceNotCreatedException
 * <b>(since NamespaceNotCreatedException is a RuntimeException)<b/>
 */
public class ManagedNamespaceNotCreatedException extends Exception {
  public ManagedNamespaceNotCreatedException(Throwable cause) {
    super(cause);
  }

  public ManagedNamespaceNotCreatedException(String msg, Throwable cause) {
    super(msg, cause);
  }
}

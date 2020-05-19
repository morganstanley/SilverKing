package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.cloud.dht.client.gen.NonVirtual;

/**
 * Thrown when namespace modification fails.
 */
@NonVirtual
public class NamespaceModificationException extends OperationException {

  public NamespaceModificationException() {
    super();
  }

  public NamespaceModificationException(String message) {
    super(message);
  }

  public NamespaceModificationException(Throwable cause) {
    super(cause);
  }

  public NamespaceModificationException(String message, Throwable cause) {
    super(message, cause);
  }

  @Override
  public String getDetailedFailureMessage() {
    return super.getMessage();
  }
}

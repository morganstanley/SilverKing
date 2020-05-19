package com.ms.silverking.cloud.dht.common;

public class NamespacePropertiesDeleteException extends NamespacePropertiesOperationException {
  public NamespacePropertiesDeleteException() {
    super();
  }

  public NamespacePropertiesDeleteException(String message) {
    super(message);
  }

  public NamespacePropertiesDeleteException(Throwable cause) {
    super(cause);
  }

  public NamespacePropertiesDeleteException(String message, Throwable cause) {
    super(message, cause);
  }

  public NamespacePropertiesDeleteException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
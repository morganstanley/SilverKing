package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import com.ms.silverking.cloud.dht.ForwardingMode;

/**
 * Retrieval protocol for distributed storage.
 */
public interface RetrievalProtocol {
  public RetrievalOperation createRetrievalOperation(long deadline,
      RetrievalOperationContainer retrievalOperationContainer, ForwardingMode forwardingMode);
}

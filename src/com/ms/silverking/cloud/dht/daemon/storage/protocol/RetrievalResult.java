package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import java.nio.ByteBuffer;
import java.util.Collection;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyedResult;
import com.ms.silverking.cloud.dht.common.OpResult;

public class RetrievalResult extends KeyedResult {
  private final ByteBuffer value;

  public RetrievalResult(DHTKey key, OpResult result, ByteBuffer value) {
    super(key, result);
    this.value = value;
  }

  public ByteBuffer getValue() {
    return value;
  }

  public int getResultLength() {
    return value != null ? value.limit() : 0;
  }

  @Override
  public String toString() {
    return super.toString() + ":" + value;
  }

  public static int totalResultLength(Collection<RetrievalResult> results) {
    int totalResultLength;

    totalResultLength = 0;
    for (RetrievalResult retrievalResult : results) {
      //System.out.printf("%s\t%d\n", retrievalResult, retrievalResult.getResultLength());
      totalResultLength += retrievalResult.getResultLength();
    }
    return totalResultLength;
  }
}

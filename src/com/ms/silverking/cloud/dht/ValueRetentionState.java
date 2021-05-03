package com.ms.silverking.cloud.dht;

public interface ValueRetentionState {
  class Empty implements ValueRetentionState {
    private Empty() {}
  }

  Empty EMPTY = new Empty();
}

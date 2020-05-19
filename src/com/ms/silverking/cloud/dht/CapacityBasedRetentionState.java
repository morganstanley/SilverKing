package com.ms.silverking.cloud.dht;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;

class CapacityBasedRetentionState implements ValueRetentionState {
  private long bytesRetained;

  @OmitGeneration
  public CapacityBasedRetentionState() {
  }

  public void addBytesRetained(long bytes) {
    bytesRetained += bytes;
  }

  public long getBytesRetained() {
    return bytesRetained;
  }
}

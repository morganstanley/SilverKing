package com.ms.silverking.cloud.dht;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.text.ObjectDefParser2;

/**
 * Least-recently written value retention policy. LRW is per key.
 */
public class LRWRetentionPolicy extends CapacityBasedRetentionPolicy {
  static final LRWRetentionPolicy template = new LRWRetentionPolicy();

  static {
    ObjectDefParser2.addParser(template);
  }

  @OmitGeneration
  public LRWRetentionPolicy(long capacityBytes) { super(capacityBytes); }

  private LRWRetentionPolicy() { this(0); }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  public static LRWRetentionPolicy parse(String def) {
    return ObjectDefParser2.parse(LRWRetentionPolicy.class, def);
  }
}

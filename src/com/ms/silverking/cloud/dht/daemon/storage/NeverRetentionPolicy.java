package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.cloud.dht.ValueRetentionPolicy;
import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.text.ObjectDefParser2;

public class NeverRetentionPolicy implements ValueRetentionPolicy {
  static final NeverRetentionPolicy template = new NeverRetentionPolicy();

  static {
    ObjectDefParser2.addParser(template);
  }

  @OmitGeneration
  public NeverRetentionPolicy() {
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  public static NeverRetentionPolicy parse(String def) {
    return ObjectDefParser2.parse(NeverRetentionPolicy.class, def);
  }
}

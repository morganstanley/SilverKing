package com.ms.silverking.cloud.dht;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.text.ObjectDefParser2;

public class PermanentRetentionPolicy implements ValueRetentionPolicy {
  static final PermanentRetentionPolicy template = new PermanentRetentionPolicy();

  static {
    ObjectDefParser2.addParser(template);
  }

  @OmitGeneration
  public PermanentRetentionPolicy() {
  }

  @Override
  public int hashCode() {
    return this.getClass().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) return false;
    return other.getClass().equals(getClass());
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  public static PermanentRetentionPolicy parse(String def) {
    return ObjectDefParser2.parse(PermanentRetentionPolicy.class, def);
  }
}

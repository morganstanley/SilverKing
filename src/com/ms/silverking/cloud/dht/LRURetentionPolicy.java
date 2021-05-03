package com.ms.silverking.cloud.dht;

import java.util.Set;

import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.text.ObjectDefParser2;

/**
 * Simple LRU value retention policy. LRU is per key.
 */
public class LRURetentionPolicy extends CapacityBasedRetentionPolicy {
  private static final LRURetentionPolicy template;
  private static final Set<String> optionalFields;
  public static final int DO_NOT_PERSIST = -1;

  static {
    template = new LRURetentionPolicy();
    optionalFields = Sets.newHashSet("persistenceIntervalSecs");
    ObjectDefParser2.addParserWithOptionalFields(template, optionalFields);
  }

  private final int persistenceIntervalSecs;

  @OmitGeneration
  public LRURetentionPolicy() {
    this(0, 300);
  }

  public LRURetentionPolicy(long capacityBytes, int persistenceIntervalSecs) {
    super(capacityBytes);
    this.persistenceIntervalSecs = persistenceIntervalSecs;
  }

  public int getPersistenceIntervalSecs() {
    return persistenceIntervalSecs;
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  @Override
  public int hashCode() {
    return Longs.hashCode(capacityBytes) ^ Ints.hashCode(persistenceIntervalSecs);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) return false;
    if (o.getClass() != getClass()) return false;

    LRURetentionPolicy other;

    other = (LRURetentionPolicy) o;
    return other.capacityBytes == capacityBytes && other.persistenceIntervalSecs == persistenceIntervalSecs;
  }

  public static LRURetentionPolicy parse(String def) {
    return ObjectDefParser2.parse(LRURetentionPolicy.class, def);
  }
}

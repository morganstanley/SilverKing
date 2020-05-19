package com.ms.silverking.cloud.dht.daemon.storage.fsm;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.numeric.NumConversion;

public class FSMInvalidatedOffsetsElement extends LTVElement {
  private static final int numElementsOffset = valueOffset;
  private static final int elementOffsetsOffset = valueOffset + NumConversion.BYTES_PER_INT;

  // for each element
  private static final int offsetOffset = 0;

  FSMInvalidatedOffsetsElement(ByteBuffer buf) {
    super(buf);
  }

  public int getNumElements() {
    return buf.getInt(numElementsOffset);
  }

  public Set<Integer> getInvalidatedOffsets() {
    Set<Integer> invalidatedOffsets;

    invalidatedOffsets = new HashSet<>();
    for (int i = 0; i < getNumElements(); i++) {
      int offset;

      offset = buf.getInt(elementOffsetsOffset + offsetOffset);
      invalidatedOffsets.add(offset);
    }
    return ImmutableSet.copyOf(invalidatedOffsets);
  }
}

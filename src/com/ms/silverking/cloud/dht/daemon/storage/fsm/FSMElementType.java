package com.ms.silverking.cloud.dht.daemon.storage.fsm;

public enum FSMElementType {
  Header, OffsetMap, OffsetLists, InvalidatedOffsets, LengthMap, Unknown;

  public static FSMElementType typeForOrdinal(int ordinal) {
    if (ordinal >= FSMElementType.values().length) {
      return Unknown;
    } else {
      return FSMElementType.values()[ordinal];
    }
  }
}

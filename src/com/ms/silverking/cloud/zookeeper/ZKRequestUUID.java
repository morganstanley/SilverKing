package com.ms.silverking.cloud.zookeeper;

import java.util.UUID;

import com.ms.silverking.id.UUIDBase;

/**
 * UUID of a zookeeper request.
 */
final class ZKRequestUUID extends UUIDBase {
  public ZKRequestUUID() {
    super();
  }

  public ZKRequestUUID(long msb, long lsb) {
    super(msb, lsb);
  }

  public ZKRequestUUID(UUID uuid) {
    super(uuid);
  }

  public static ZKRequestUUID fromString(String uuid) {
    return new ZKRequestUUID(UUID.fromString(uuid));
  }
}

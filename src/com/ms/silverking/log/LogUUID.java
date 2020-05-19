package com.ms.silverking.log;

import java.util.UUID;

import com.ms.silverking.id.UUIDBase;

/**
 *
 */
public class LogUUID extends UUIDBase {

  public LogUUID() {
    super();
  }

  public LogUUID(UUID uuid) {
    super(uuid);
  }

  public static LogUUID fromString(String uuid) {
    return new LogUUID(UUID.fromString(uuid));
  }

}

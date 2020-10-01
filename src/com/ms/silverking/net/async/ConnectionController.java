package com.ms.silverking.net.async;

public interface ConnectionController {
  /**
   * This will disconnect all connections
   *
   * @param reason
   * @return number of connections disconnected
   */
  public int disconnectAll(String reason);
}

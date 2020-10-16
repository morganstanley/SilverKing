package com.ms.silverking.thread.lwt.util;

public interface Listener<T> {
  public void notification(Broadcaster<T> broadcaster, T message);
}

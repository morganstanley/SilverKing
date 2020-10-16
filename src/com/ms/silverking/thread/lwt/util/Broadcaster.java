package com.ms.silverking.thread.lwt.util;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import com.ms.silverking.thread.lwt.BaseWorker;

public class Broadcaster<T> extends BaseWorker<T> {
  private Set<Listener<T>>  listeners;
  
  public Broadcaster() {
    this.listeners = new ConcurrentSkipListSet<>();
  }
  
  public void addListener(Listener<T> listener) {
    listeners.add(listener);
  }
  
  public void notifyListeners(T message) {
    addWork(message, 0);
  }
  
  @Override
  public void doWork(T notification) {
    for (Listener<T> listener : listeners) {
      listener.notification(this, notification);
    }
  }
}

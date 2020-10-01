package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.cloud.dht.client.gen.NonVirtual;

@NonVirtual
public class SessionClosedException extends ClientException {
  public SessionClosedException() {
    super();
  }

  public SessionClosedException(String arg0, Throwable arg1) {
    super(arg0, arg1);
  }

  public SessionClosedException(String arg0) {
    super(arg0);
  }

  public SessionClosedException(Throwable arg0) {
    super(arg0);
  }
}
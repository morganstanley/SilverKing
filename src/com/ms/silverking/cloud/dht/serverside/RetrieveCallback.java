package com.ms.silverking.cloud.dht.serverside;

import java.util.function.Function;

@FunctionalInterface
public interface RetrieveCallback<T, U> extends Function<T, U> {
  static <T> RetrieveCallback<T, T> identity() {
    return t -> t;
  }
}
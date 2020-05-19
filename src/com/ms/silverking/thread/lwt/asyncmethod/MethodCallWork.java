package com.ms.silverking.thread.lwt.asyncmethod;

class MethodCallWork {
  final String methodName;
  final Object[] parameters;

  MethodCallWork(String methodName, Object[] parameters) {
    this.methodName = methodName;
    this.parameters = parameters;
  }
}

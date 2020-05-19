package com.ms.silverking.thread.lwt.asyncmethod;

import java.lang.reflect.Method;

import com.ms.silverking.thread.lwt.BaseWorker;
import com.ms.silverking.thread.lwt.LWTPool;
import com.ms.silverking.thread.lwt.LWTPoolParameters;
import com.ms.silverking.thread.lwt.LWTPoolProvider;

public class MethodCallWorker extends BaseWorker<MethodCallWork> {
  private final Object target;

  public MethodCallWorker(LWTPool methodCallPool, Object target) {
    super(methodCallPool, true, 0);
    this.target = target;
  }

  public MethodCallWorker(LWTPoolParameters poolParams, Object target) {
    this(LWTPoolProvider.createPool(poolParams), target);
  }

  public void asyncInvocation(String methodName, Object... parameters) {
    addWork(new MethodCallWork(methodName, parameters));
  }

  @Override
  public void doWork(MethodCallWork mcw) {
    Method method;

    try {
      method = target.getClass().getMethod(mcw.methodName, parameterTypes(mcw.parameters));
      method.invoke(target, mcw.parameters);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Class<?>[] parameterTypes(Object[] parameters) {
    Class<?>[] types;

    types = new Class[parameters.length];
    for (int i = 0; i < parameters.length; i++) {
      types[i] = parameters[i].getClass();
    }
    return types;
  }
}

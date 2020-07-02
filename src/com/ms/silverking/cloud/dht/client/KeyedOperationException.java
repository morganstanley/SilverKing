package com.ms.silverking.cloud.dht.client;

import com.google.common.collect.ImmutableMap;
import com.ms.silverking.cloud.dht.client.gen.NonVirtual;

import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

/**
 * Thrown when a keyed client-initiated operation fails. OperationState is provided
 * on a key-by-key basis, but may be incomplete. At least one key will have error
 * information available.
 */
@NonVirtual
public abstract class KeyedOperationException extends OperationException {
  private final Map<Object, OperationState> operationState;
  private final Map<Object, FailureCause> failureCause;
  private final Set<Object> failedKeys;

  private static final String failuresDelimiter = ",";
  private static final String keyValueDelimiter = ":";
  private static final int keysLimit = 10;

  private static String createFailureMessage(Map<Object, FailureCause> failureCause, String delimiter, int keysLimit) {
    // Jace can't handle InvokeDynamic - rewrite without it
        /*
        StringJoiner errorMessageJoiner = new StringJoiner(delimiter);
        failureCause.entrySet().stream().limit(keysLimit).forEach((entry) -> {
            errorMessageJoiner.add(
                    String.format("%s%s%s", entry.getKey().toString(), keyValueDelimiter, entry.getValue().name()));
        });
        return errorMessageJoiner.toString();
        */
    StringBuffer sb;
    int limitCount;

    sb = new StringBuffer();
    limitCount = 0;
    for (Map.Entry<Object, FailureCause> entry : failureCause.entrySet()) {
      sb.append(String.format("%s%s%s", entry.getKey().toString(), keyValueDelimiter, entry.getValue().name()));
      if (++limitCount >= keysLimit) {
        break;
      }
    }
    return sb.toString();
  }

  protected KeyedOperationException(Map<Object, OperationState> operationState,
      Map<Object, FailureCause> failureCause) {
    super(createFailureMessage(failureCause, failuresDelimiter, keysLimit));
    this.operationState = ImmutableMap.copyOf(operationState);
    this.failureCause = ImmutableMap.copyOf(failureCause);
    this.failedKeys = failureCause.keySet();
  }

  protected KeyedOperationException(String message, Map<Object, OperationState> operationState,
      Map<Object, FailureCause> failureCause) {
    super(message);
    this.operationState = ImmutableMap.copyOf(operationState);
    this.failureCause = ImmutableMap.copyOf(failureCause);
    this.failedKeys = failureCause.keySet();
  }

  protected KeyedOperationException(Throwable cause, Map<Object, OperationState> operationState,
      Map<Object, FailureCause> failureCause) {
    super(cause);
    this.operationState = ImmutableMap.copyOf(operationState);
    this.failureCause = ImmutableMap.copyOf(failureCause);
    this.failedKeys = failureCause.keySet();
  }

  protected KeyedOperationException(String message, Throwable cause, Map<Object, OperationState> operationState,
      Map<Object, FailureCause> failureCause) {
    super(message, cause);
    this.operationState = ImmutableMap.copyOf(operationState);
    this.failureCause = ImmutableMap.copyOf(failureCause);
    this.failedKeys = failureCause.keySet();
  }

  public Map<Object, OperationState> getOperationState() {
    return operationState;
  }

  public OperationState getOperationState(Object key) {
    return operationState.get(key);
  }

  public Map<Object, FailureCause> getFailureCause() {
    return failureCause;
  }

  public FailureCause getFailureCause(Object key) {
    return failureCause.get(key);
  }

  public Set<Object> getFailedKeys() {
    return failedKeys;
  }

  public static String getFailuresDelimiter() {
    return failuresDelimiter;
  }

  public static String getKeyValueDelimiter() {
    return keyValueDelimiter;
  }

  public static int getKeysLimit() {
    return keysLimit;
  }

  public String getDetailedFailureMessage() {
    return createFailureMessage(getFailureCause(), "\n", Integer.MAX_VALUE);
  }
}

package com.ms.silverking.cloud.dht;

/**
 * Specifies the response of a WaitFor operation to a timeout. Exit quietly or throw an exception.
 */
public enum AllReplicasExcludedResponse {
  /**
   * Throw an exception when all replicas are excluded
   */
  EXCEPTION,
  /**
   * Ignore the fact that all replicas are excluded;
   * If a replica appears eventually, allow the operation to succeed
   */
  IGNORE;

  /**
   * By default, throw an exception when all replicas are excluded
   */
  public static final AllReplicasExcludedResponse defaultResponse = EXCEPTION;
}

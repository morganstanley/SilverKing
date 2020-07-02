package com.ms.silverking.cloud.dht.common;

import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.NonExistenceResponse;
import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.WaitMode;
import com.ms.silverking.cloud.dht.serverside.SSRetrievalOptions;
import com.ms.silverking.cloud.dht.trace.TraceIDProvider;

import java.util.Optional;

/**
 * Adds internally useful information to RetrievalOptions that should not be exposed to
 * end users. Also removes RetrievalOptions that only apply in the client.
 */
public class InternalRetrievalOptions implements SSRetrievalOptions {
  private final RetrievalOptions retrievalOptions;
  private final boolean verifyIntegrity;
  private final ConsistencyProtocol cpSSToVerify; // ConsistencyProtocol to verify storage state against
  // non-null value implies that state should be verified
  private final byte[] maybeTraceID;

  public InternalRetrievalOptions(RetrievalOptions retrievalOptions, boolean verifyIntegrity,
      ConsistencyProtocol cpSSToVerify, byte[] maybeTraceID) {
    this.retrievalOptions = retrievalOptions;
    this.verifyIntegrity = verifyIntegrity;
    this.cpSSToVerify = cpSSToVerify;
    this.maybeTraceID = maybeTraceID;
  }

  public InternalRetrievalOptions(RetrievalOptions retrievalOptions, boolean verifyIntegrity,
      ConsistencyProtocol cpSSToVerify) {
    this(retrievalOptions, verifyIntegrity, cpSSToVerify, TraceIDProvider.noTraceID);
  }

  public InternalRetrievalOptions(RetrievalOptions retrievalOptions, boolean verifyIntegrity) {
    this(retrievalOptions, verifyIntegrity, null);
  }

  public InternalRetrievalOptions(RetrievalOptions retrievalOptions) {
    this(retrievalOptions, false, null);
  }

  public static InternalRetrievalOptions fromSSRetrievalOptions(SSRetrievalOptions options) {
    if (options instanceof InternalRetrievalOptions) {
      return (InternalRetrievalOptions) options;
    } else {
      RetrievalOptions retrievalOptions;

      retrievalOptions = new RetrievalOptions(null, null, options.getRetrievalType(), WaitMode.GET,
          options.getVersionConstraint(), null, options.getVerifyIntegrity(), options.getReturnInvalidations(), null,
          false, null, null);
      return new InternalRetrievalOptions(retrievalOptions, options.getVerifyIntegrity());
    }
  }

  public InternalRetrievalOptions retrievalOptions(RetrievalOptions retrievalOptions) {
    return new InternalRetrievalOptions(retrievalOptions, verifyIntegrity, cpSSToVerify);
  }

  public InternalRetrievalOptions retrievalType(RetrievalType retrievalType) {
    return retrievalOptions(retrievalOptions.retrievalType(retrievalType));
  }

  public RetrievalOptions getRetrievalOptions() {
    return retrievalOptions;
  }

  @Override
  public boolean getVerifyIntegrity() {
    return verifyIntegrity;
  }

  public ConsistencyProtocol getCPSSToVerify() {
    return cpSSToVerify;
  }

  public boolean getVerifyStorageState() {
    return cpSSToVerify != null;
  }

  @Override
  public boolean getReturnInvalidations() {
    return retrievalOptions.getReturnInvalidations();
  }

  /**
   * @return
   */
  @Override
  public RetrievalType getRetrievalType() {
    return retrievalOptions.getRetrievalType();
  }

  /**
   * waitMode getter
   *
   * @return waidMode
   */
  public final WaitMode getWaitMode() {
    return retrievalOptions.getWaitMode();
  }

  /**
   * versionConstraint getter
   *
   * @return
   */
  @Override
  public final VersionConstraint getVersionConstraint() {
    return retrievalOptions.getVersionConstraint();
  }

  /**
   * nonexistenceResponse getter
   *
   * @return
   */
  public final NonExistenceResponse getNonExistenceResponse() {
    return retrievalOptions.getNonExistenceResponse();
  }

  /**
   * userOptions getter
   *
   * @return
   */
  @Override
  public final byte[] getUserOptions() {
    return retrievalOptions.getUserOptions();
  }

  @Override
  public final byte[] getAuthorizationUser() {
    return retrievalOptions.getAuthorizationUser();
  }

  public final InternalRetrievalOptions authorizedAs(byte[] user) {
    RetrievalOptions opt = retrievalOptions.authorizationUser(user);
    return new InternalRetrievalOptions(opt, verifyIntegrity, cpSSToVerify, maybeTraceID);
  }

  @Override
  public Optional<byte[]> getTraceID() {
    if (TraceIDProvider.isValidTraceID(maybeTraceID)) {
      return Optional.of(maybeTraceID);
    } else {
      return Optional.empty();
    }
  }

  /**
   * Return copy of this object with modified VersionConstraint
   *
   * @param vc
   * @return
   */
  public InternalRetrievalOptions versionConstraint(VersionConstraint vc) {
    return new InternalRetrievalOptions(retrievalOptions.versionConstraint(vc), verifyIntegrity, cpSSToVerify,
        maybeTraceID);
  }

  /**
   * Return copy of this object with modified WaitMode
   *
   * @param waitMode
   * @return
   */
  public InternalRetrievalOptions waitMode(WaitMode waitMode) {
    return new InternalRetrievalOptions(retrievalOptions.waitMode(waitMode), verifyIntegrity, cpSSToVerify,
        maybeTraceID);
  }

  /**
   * Return copy of this object with modified verifyStorageState
   *
   * @param cpSSToVerify
   * @return
   */
  public InternalRetrievalOptions cpSSToVerify(ConsistencyProtocol cpSSToVerify) {
    return new InternalRetrievalOptions(retrievalOptions, verifyIntegrity, cpSSToVerify, maybeTraceID);
  }

  public InternalRetrievalOptions maybeTraceID(byte[] maybeTraceID) {
    return new InternalRetrievalOptions(retrievalOptions, verifyIntegrity, cpSSToVerify, maybeTraceID);
  }

  @Override
  public int hashCode() {
    throw new RuntimeException("not implemented");
  }

  @Override
  public boolean equals(Object other) {
    throw new RuntimeException("not implemented");
  }

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append(retrievalOptions);
    sb.append(':');
    sb.append(verifyIntegrity);
    return sb.toString();
  }
}

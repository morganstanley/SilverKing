package com.ms.silverking.cloud.dht;

import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.client.OpTimeoutController;
import com.ms.silverking.cloud.dht.trace.TraceIDProvider;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.OptionsHelper;
import com.ms.silverking.cloud.dht.net.ForwardingMode;
import com.ms.silverking.text.ObjectDefParser2;

/**
 * Options for Get operations. (RetrievalOptions with WaitMode fixed at GET.)
 */
public final class GetOptions extends RetrievalOptions {
  private static final Set<String> exclusionFields = ImmutableSet.of("waitMode");

  private static final GetOptions template = OptionsHelper.newGetOptions(DHTConstants.standardTimeoutController,
      RetrievalType.VALUE, VersionConstraint.defaultConstraint);

  static {
    ObjectDefParser2.addParserWithExclusions(template, exclusionFields);
  }

  ///
  /// FIXME - this is C++ only.
  /// This should be removed once C++ SKGetOptions.cpp is using the other constructor below properly.
  ///

  /**
   * Construct a fully-specified GetOptions.
   * Usage should be avoided; an instance should be obtained and modified from an enclosing environment.
   *
   * @param opTimeoutController     opTimeoutController for the operation
   * @param secondaryTargets        constrains queried secondary replicas
   *                                to operation solely on the node that receives this operation
   * @param retrievalType           type of retrieval
   * @param versionConstraint       specify the version
   * @param nonExistenceResponse    action to perform for non-existent keys
   * @param verifyChecksums         whether or not to verify checksums
   * @param returnInvalidations     normally false, true causes invalidated values to be returned.
   *                                only valid for META_DATA retrievals
   * @param forwardingMode          FORWARD is for normal operation. DO_NOT_FORWARD restricts the get
   *                                to the receiving node
   * @param updateSecondariesOnMiss update secondary replicas when a value is not found at the
   *                                replica, but is found at the primary
   */
  public GetOptions(OpTimeoutController opTimeoutController, Set<SecondaryTarget> secondaryTargets,
      RetrievalType retrievalType, VersionConstraint versionConstraint, NonExistenceResponse nonExistenceResponse,
      boolean verifyChecksums, boolean returnInvalidations, ForwardingMode forwardingMode,
      boolean updateSecondariesOnMiss) {
    super(opTimeoutController, secondaryTargets, retrievalType, WaitMode.GET, versionConstraint, nonExistenceResponse,
        verifyChecksums, returnInvalidations, forwardingMode, false, null, null);
  }

  public GetOptions(OpTimeoutController opTimeoutController, Set<SecondaryTarget> secondaryTargets,
      RetrievalType retrievalType, VersionConstraint versionConstraint, NonExistenceResponse nonExistenceResponse,
      boolean verifyChecksums, boolean returnInvalidations, ForwardingMode forwardingMode,
      boolean updateSecondariesOnMiss, byte[] userOptions, byte[] authorizationUser) {
    this(opTimeoutController, secondaryTargets, DHTConstants.defaultTraceIDProvider, retrievalType, versionConstraint,
        nonExistenceResponse, verifyChecksums, returnInvalidations, forwardingMode, updateSecondariesOnMiss,
        userOptions, authorizationUser);
  }

  /**
   * Construct a fully-specified GetOptions. (Complete constructor for reflection)
   * Usage should be avoided; an instance should be obtained and modified from an enclosing environment.
   *
   * @param opTimeoutController     opTimeoutController for the operation
   * @param secondaryTargets        constrains queried secondary replicas
   *                                to operation solely on the node that receives this operation
   * @param traceIDProvider         trace provider for message group
   * @param retrievalType           type of retrieval
   * @param versionConstraint       specify the version
   * @param nonExistenceResponse    action to perform for non-existent keys
   * @param verifyChecksums         whether or not to verify checksums
   * @param returnInvalidations     normally false, true causes invalidated values to be returned.
   *                                only valid for META_DATA retrievals
   * @param updateSecondariesOnMiss update secondary replicas when a value is not found at the
   *                                replica, but is found at the primary
   * @param userOptions             side channel for user options that can be handled with custom logic
   * @param authorizationUser       a username which may be required by an authorization plugin on the server
   */
  public GetOptions(OpTimeoutController opTimeoutController, Set<SecondaryTarget> secondaryTargets,
      TraceIDProvider traceIDProvider, RetrievalType retrievalType, VersionConstraint versionConstraint,
      NonExistenceResponse nonExistenceResponse, boolean verifyChecksums, boolean returnInvalidations,
      ForwardingMode forwardingMode, boolean updateSecondariesOnMiss, byte[] userOptions, byte[] authorizationUser) {
    super(opTimeoutController, secondaryTargets, traceIDProvider, retrievalType, WaitMode.GET, versionConstraint,
        nonExistenceResponse, verifyChecksums, returnInvalidations, forwardingMode, updateSecondariesOnMiss,
        userOptions, authorizationUser);
  }

  /**
   * Return a GetOptions instance like this instance, but with a new OpTimeoutController.
   *
   * @param opTimeoutController the new field value
   * @return the modified GetOptions
   */
  public GetOptions opTimeoutController(OpTimeoutController opTimeoutController) {
    return new GetOptions(opTimeoutController, getSecondaryTargets(), getTraceIDProvider(), getRetrievalType(),
        getVersionConstraint(), getNonExistenceResponse(), getVerifyChecksums(), getReturnInvalidations(),
        getForwardingMode(), getUpdateSecondariesOnMiss(), getUserOptions(), getAuthorizationUser());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new SecondaryTargets.
   *
   * @param secondaryTargets the new field value
   * @return the modified GetOptions
   */
  public GetOptions secondaryTargets(Set<SecondaryTarget> secondaryTargets) {
    return new GetOptions(getOpTimeoutController(), secondaryTargets, getTraceIDProvider(), getRetrievalType(),
        getVersionConstraint(), getNonExistenceResponse(), getVerifyChecksums(), getReturnInvalidations(),
        getForwardingMode(), getUpdateSecondariesOnMiss(), getUserOptions(), getAuthorizationUser());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new SecondaryTargets.
   *
   * @param secondaryTarget the new field value
   * @return the modified GetOptions
   */
  public GetOptions secondaryTargets(SecondaryTarget secondaryTarget) {
    Preconditions.checkNotNull(secondaryTarget);
    return new GetOptions(getOpTimeoutController(), ImmutableSet.of(secondaryTarget), getTraceIDProvider(),
        getRetrievalType(), getVersionConstraint(), getNonExistenceResponse(), getVerifyChecksums(),
        getReturnInvalidations(), getForwardingMode(), getUpdateSecondariesOnMiss(), getUserOptions(), getAuthorizationUser());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new traceIDProvider.
   *
   * @param traceIDProvider the new field value
   * @return the modified GetOptions
   */
  public GetOptions traceIDProvider(TraceIDProvider traceIDProvider) {
    Preconditions.checkNotNull(traceIDProvider);
    return new GetOptions(getOpTimeoutController(), getSecondaryTargets(), traceIDProvider, getRetrievalType(),
        getVersionConstraint(), getNonExistenceResponse(), getVerifyChecksums(), getReturnInvalidations(),
        getForwardingMode(), getUpdateSecondariesOnMiss(), getUserOptions(), getAuthorizationUser());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new RetrievalType.
   *
   * @param retrievalType the new field value
   * @return the modified GetOptions
   */
  public GetOptions retrievalType(RetrievalType retrievalType) {
    return new GetOptions(getOpTimeoutController(), getSecondaryTargets(), getTraceIDProvider(), retrievalType,
        getVersionConstraint(), getNonExistenceResponse(), getVerifyChecksums(), getReturnInvalidations(),
        getForwardingMode(), getUpdateSecondariesOnMiss(), getUserOptions(), getAuthorizationUser());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new VersionConstraint.
   *
   * @param versionConstraint the new field value
   * @return the modified GetOptions
   */
  public GetOptions versionConstraint(VersionConstraint versionConstraint) {
    return new GetOptions(getOpTimeoutController(), getSecondaryTargets(), getTraceIDProvider(), getRetrievalType(),
        versionConstraint, getNonExistenceResponse(), getVerifyChecksums(), getReturnInvalidations(),
        getForwardingMode(), getUpdateSecondariesOnMiss(), getUserOptions(), getAuthorizationUser());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new NonExistenceResponse.
   *
   * @param nonExistenceResponse the new field value
   * @return the modified GetOptions
   */
  public GetOptions nonExistenceResponse(NonExistenceResponse nonExistenceResponse) {
    return new GetOptions(getOpTimeoutController(), getSecondaryTargets(), getTraceIDProvider(), getRetrievalType(),
        getVersionConstraint(), nonExistenceResponse, getVerifyChecksums(), getReturnInvalidations(),
        getForwardingMode(), getUpdateSecondariesOnMiss(), getUserOptions(), getAuthorizationUser());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new verifyChecksums.
   *
   * @param verifyChecksums the new field value
   * @return the modified GetOptions
   */
  public GetOptions verifyChecksums(boolean verifyChecksums) {
    return new GetOptions(getOpTimeoutController(), getSecondaryTargets(), getTraceIDProvider(), getRetrievalType(),
        getVersionConstraint(), getNonExistenceResponse(), verifyChecksums, getReturnInvalidations(),
        getForwardingMode(), getUpdateSecondariesOnMiss(), getUserOptions(), getAuthorizationUser());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new returnInvalidations.
   *
   * @param returnInvalidations the new field value
   * @return the modified GetOptions
   */
  public GetOptions returnInvalidations(boolean returnInvalidations) {
    return new GetOptions(getOpTimeoutController(), getSecondaryTargets(), getTraceIDProvider(), getRetrievalType(),
        getVersionConstraint(), getNonExistenceResponse(), getVerifyChecksums(), returnInvalidations,
        getForwardingMode(), getUpdateSecondariesOnMiss(), getUserOptions(), getAuthorizationUser());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new updateSecondariesOnMiss.
   *
   * @param updateSecondariesOnMiss the new field value
   * @return the modified GetOptions
   */
  public GetOptions updateSecondariesOnMiss(boolean updateSecondariesOnMiss) {
    return new GetOptions(getOpTimeoutController(), getSecondaryTargets(), getTraceIDProvider(), getRetrievalType(),
        getVersionConstraint(), getNonExistenceResponse(), getVerifyChecksums(), getReturnInvalidations(),
        getForwardingMode(), updateSecondariesOnMiss, getUserOptions(), getAuthorizationUser());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new forwardingMode.
   *
   * @param forwardingMode the new field value
   * @return the modified GetOptions
   */
  public GetOptions forwardingMode(ForwardingMode forwardingMode) {
    return new GetOptions(getOpTimeoutController(), getSecondaryTargets(), getTraceIDProvider(), getRetrievalType(),
        getVersionConstraint(), getNonExistenceResponse(), getVerifyChecksums(), getReturnInvalidations(),
        forwardingMode, getUpdateSecondariesOnMiss(), getUserOptions(), getAuthorizationUser());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new userOptions.
   *
   * @param userOptions the new field value
   * @return the modified GetOptions
   */
  public GetOptions userOptions(byte[] userOptions) {
    return new GetOptions(getOpTimeoutController(), getSecondaryTargets(), getTraceIDProvider(), getRetrievalType(),
        getVersionConstraint(), getNonExistenceResponse(), getVerifyChecksums(), getReturnInvalidations(),
        getForwardingMode(), getUpdateSecondariesOnMiss(), userOptions, getAuthorizationUser());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new authorizationUser.
   *
   * @param authorizationUser the new field value
   * @return the modified GetOptions
   */
  public GetOptions authorizationUser(byte[] authorizationUser) {
    return new GetOptions(getOpTimeoutController(), getSecondaryTargets(), getTraceIDProvider(), getRetrievalType(),
        getVersionConstraint(), getNonExistenceResponse(), getVerifyChecksums(), getReturnInvalidations(),
        getForwardingMode(), getUpdateSecondariesOnMiss(), getUserOptions(), authorizationUser);
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }
}

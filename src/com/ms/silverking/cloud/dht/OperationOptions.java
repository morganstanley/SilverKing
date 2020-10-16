package com.ms.silverking.cloud.dht;

import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.ms.silverking.cloud.dht.client.OpTimeoutController;
import com.ms.silverking.cloud.dht.trace.TraceIDProvider;
import com.ms.silverking.object.ObjectUtil;
import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;

public class OperationOptions {
  private final OpTimeoutController opTimeoutController;
  private final Set<SecondaryTarget> secondaryTargets;
  private final TraceIDProvider traceIDProvider;
  private final AllReplicasExcludedResponse allReplicasExcludedResponse;

  static {
    ObjectDefParser2.addParserWithExclusions(OperationOptions.class, null, FieldsRequirement.ALLOW_INCOMPLETE, null);
  }

  public OperationOptions(OpTimeoutController opTimeoutController, Set<SecondaryTarget> secondaryTargets,
      TraceIDProvider traceIDProvider, AllReplicasExcludedResponse allReplicasExcludedResponse) {
    Preconditions.checkNotNull(opTimeoutController);
    Preconditions.checkNotNull(traceIDProvider);
    this.opTimeoutController = opTimeoutController;
    this.secondaryTargets = secondaryTargets;
    this.traceIDProvider = traceIDProvider;
    this.allReplicasExcludedResponse = allReplicasExcludedResponse;
  }

  public final boolean hasTraceID() {
    return traceIDProvider.isEnabled();
  }

  public OperationOptions opTimeoutController(OpTimeoutController opTimeoutController) {
    return new OperationOptions(opTimeoutController, secondaryTargets, traceIDProvider, allReplicasExcludedResponse);
  }
  
  public OperationOptions traceIDProvider(Set<SecondaryTarget> secondaryTargets) {
    return new OperationOptions(opTimeoutController, secondaryTargets, traceIDProvider, allReplicasExcludedResponse);
  }
  
  public OperationOptions traceIDProvider(TraceIDProvider traceIDProvider) {
    return new OperationOptions(opTimeoutController, secondaryTargets, traceIDProvider, allReplicasExcludedResponse);
  }

  public OperationOptions allReplicasExcludedResponse(AllReplicasExcludedResponse allReplicasExcludedResponse) {
    return new OperationOptions(opTimeoutController, secondaryTargets, traceIDProvider, allReplicasExcludedResponse);
  }
  
  public TraceIDProvider getTraceIDProvider() {
    return traceIDProvider;
  }

  public OpTimeoutController getOpTimeoutController() {
    return opTimeoutController;
  }

  public Set<SecondaryTarget> getSecondaryTargets() {
    return secondaryTargets;
  }
  
  /**
   * Return allReplicasExcludedResponse
   *
   * @return allReplicasExcludedResponse
   */
  public AllReplicasExcludedResponse getAllReplicasExcludedResponse() {
    return allReplicasExcludedResponse;
  }

  @Override
  public int hashCode() {
    int hashCode;

    hashCode = opTimeoutController.hashCode();
    if (secondaryTargets != null) {
      hashCode = hashCode ^ secondaryTargets.hashCode() ^ ObjectUtil.hashCode(traceIDProvider) ^ allReplicasExcludedResponse.hashCode();
    }
    return hashCode;
  }

  @Override
  public boolean equals(Object other) {
    OperationOptions oOptions;

    oOptions = (OperationOptions) other;
    return Objects.equals(opTimeoutController, oOptions.opTimeoutController) && Objects.equals(secondaryTargets,
        oOptions.secondaryTargets) && Objects.equals(traceIDProvider, oOptions.traceIDProvider)
        && oOptions.allReplicasExcludedResponse == allReplicasExcludedResponse;
  }
}

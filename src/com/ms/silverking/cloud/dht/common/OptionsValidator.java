package com.ms.silverking.cloud.dht.common;

import com.ms.silverking.cloud.dht.PutOptions;

public class OptionsValidator {
  public static void validatePutOptions(PutOptions o) {
    if (o.getRequiredPreviousVersion() < 0 && o.getRequiredPreviousVersion() != PutOptions.previousVersionNonexistent && o.getRequiredPreviousVersion() != PutOptions.previousVersionNonexistentOrInvalid) {
      throw new IllegalArgumentException("requiredPreviousVersion < 0 (and not a reserved value)");
    }
    if (o.getRequiredPreviousVersion() != PutOptions.noVersionRequired && o.getVersion() > 0 // 0 indicates version
        // to be filled in on by implementation
        && o.getVersion() <= o.getRequiredPreviousVersion()) {
      throw new IllegalArgumentException(
          "version <= requiredPreviousVersion. " + o.getVersion() + " <= " + o.getRequiredPreviousVersion());
    }
  }
}

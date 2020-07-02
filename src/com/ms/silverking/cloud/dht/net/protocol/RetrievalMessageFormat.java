package com.ms.silverking.cloud.dht.net.protocol;

import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.net.SecondaryTargetSerializer;
import com.ms.silverking.numeric.NumConversion;

public class RetrievalMessageFormat extends KeyedMessageFormat {
  public static final int stDataOffset = RetrievalResponseMessageFormat.optionBytesSize;

  public static final int getOptionsBufferLength(RetrievalOptions retrievalOptions) {
    return RetrievalResponseMessageFormat.optionBytesSize // same format for both directions
        + NumConversion.BYTES_PER_SHORT // length field for secondary targets
        + SecondaryTargetSerializer.serializedLength(retrievalOptions.getSecondaryTargets())
        + NumConversion.BYTES_PER_INT // length field for user options
        + (retrievalOptions.getUserOptions() == RetrievalOptions.noUserOptions ? 0 : retrievalOptions.getUserOptions().length)
        + NumConversion.BYTES_PER_INT // length field for requested user string
        + (retrievalOptions.getAuthorizationUser() == RetrievalOptions.noAuthorizationUser ? 0 : retrievalOptions.getAuthorizationUser().length);
  }
}

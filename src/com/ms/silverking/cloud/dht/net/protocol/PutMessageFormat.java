package com.ms.silverking.cloud.dht.net.protocol;

import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.net.SecondaryTargetSerializer;
import com.ms.silverking.numeric.NumConversion;

public class PutMessageFormat extends PutBaseMessageFormat {
  // key buffer entry

  public static final int uncompressedValueLengthSize = NumConversion.BYTES_PER_INT;
  public static final int compressedValueLengthSize = NumConversion.BYTES_PER_INT;

  public static final int uncompressedValueLengthOffset = KeyValueMessageFormat.size;
  public static final int compressedValueLengthOffset = uncompressedValueLengthOffset + uncompressedValueLengthSize;
  public static final int checksumOffset = compressedValueLengthOffset + compressedValueLengthSize;

  public static int size(ChecksumType _checksumType) {
    return checksumOffset + _checksumType.length();
  }

  // options buffer

  public static final int stDataOffset = valueCreatorOffset + valueCreatorSize;

  private static final int optionBaseBytes = versionSize * 2 + lockSecondsSize + ccssSize + valueCreatorSize;

  public static final int userDataOffset(int stLength) {
    return stDataOffset + stLength + NumConversion.BYTES_PER_SHORT;
  }

  public static final int getOptionsBufferLength(PutOptions putOptions) {
    byte[] userData;

    userData = putOptions.getUserData();
    return optionBaseBytes + NumConversion.BYTES_PER_SHORT + SecondaryTargetSerializer.serializedLength(
        putOptions.getSecondaryTargets()) + ((userData == null) ? 0 : userData.length);
  }
}

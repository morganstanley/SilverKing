package com.ms.silverking.cloud.dht.common;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.impl.SystemChecksum;
import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.text.StringUtil;
import com.ms.silverking.util.ArrayUtil;

/**
 * Methods for dealing with metadata stored as a byte array.
 * <p>
 * When stored with data, metadata is stored at the start of the byte array
 * followed by data.
 * <p>
 * Metadata and data is formatted in the array as follows:
 * <p>
 * [Key is stored first, but is not accounted for in MetaDataUtil
 * i.e. MetaDataUtil must be given the offset just after the key.]
 * <p>
 * storedLength (4)
 * uncompressedLength (4)
 * version (8)
 * creationTime (8)
 * creator (8)
 * lockSeconds (2)
 * ccss (2)         (compression type, checksum type, StorageState)
 * userDataLength (1)
 * checksum (variable)
 * data...
 * userData...
 */
public class MetaDataUtil {
  // storedLength is all bytes required to store data and metadata including checksums, userdata, etc.
  private static final int storedLengthOffset = 0;
  private static final int uncompressedLengthOffset = storedLengthOffset + NumConversion.BYTES_PER_INT;
  private static final int versionOffset = uncompressedLengthOffset + NumConversion.BYTES_PER_INT;
  private static final int creationTimeOffset = versionOffset + NumConversion.BYTES_PER_LONG;
  private static final int creatorOffset = creationTimeOffset + NumConversion.BYTES_PER_LONG;
  private static final int lockSecondsOffset = creatorOffset + ValueCreator.BYTES;
  private static final int ccss = lockSecondsOffset + NumConversion.BYTES_PER_SHORT;
  private static final int userDataLengthOffset = ccss + NumConversion.BYTES_PER_SHORT;
  // checksum if any is stored here
  private static final int dataOffset = userDataLengthOffset + 1;

  private static final int fixedMetaDataLength = dataOffset;

  public static int getMinimumEntrySize() {
    return fixedMetaDataLength;
  }

  public static int computeStoredLength(int compressedLength, int checksumLength, int userDataLength) {
    return compressedLength + checksumLength + userDataLength + fixedMetaDataLength;
  }

  public static int computeMetaDataLength(int compressedLength, int checksumLength, int userDataLength) {
    return checksumLength + userDataLength + fixedMetaDataLength;
  }

  public static short getCCSS(byte[] storedValue, int baseOffset) {
    return NumConversion.bytesToShort(storedValue, baseOffset + ccss);
    //return storedValue[baseOffset + ccss];
  }

  public static byte getCompression(byte[] storedValue, int baseOffset) {
    return CCSSUtil.getCompression(getCCSS(storedValue, baseOffset));
  }

  public static ChecksumType getChecksumType(byte[] storedValue, int baseOffset) {
    return CCSSUtil.getChecksumType(getCCSS(storedValue, baseOffset));
  }

  public byte getStorageState(byte[] storedValue, int baseOffset) {
    return CCSSUtil.getStorageState(getCCSS(storedValue, baseOffset));
  }

  public static byte[] getChecksum(ByteBuffer storedValue, int baseOffset) {
    byte[] checksum;

    checksum = new byte[getChecksumLength(storedValue, baseOffset)];
    ((ByteBuffer) storedValue.asReadOnlyBuffer().position(baseOffset + userDataLengthOffset + 1)).get(checksum);
    //System.arraycopy(storedValue, baseOffset + userDataLengthOffset + 1,
    //                 checksum, 0, checksum.length);
    return checksum;
  }

  private static int getChecksumLength(byte[] storedValue, int baseOffset) {
    ChecksumType checksumType;

    checksumType = getChecksumType(storedValue, baseOffset);
    return checksumType.length();
  }

  public static byte[] getChecksum(byte[] storedValue, int baseOffset) {
    byte[] checksum;

    checksum = new byte[getChecksumLength(storedValue, baseOffset)];
    System.arraycopy(storedValue, baseOffset + userDataLengthOffset + 1, checksum, 0, checksum.length);
    return checksum;
  }

  public static boolean isInvalidation(ChecksumType checksumType, byte[] checksum) {
    if (checksumType == ChecksumType.SYSTEM) {
      return SystemChecksum.isInvalidationChecksum(checksum);
    } else {
      return false;
    }
  }

  public static boolean isInvalidated(byte[] storedValue, int baseOffset) {
    ChecksumType checksumType;

    checksumType = getChecksumType(storedValue, baseOffset);
    if (checksumType == ChecksumType.SYSTEM) {
      byte[] actualChecksum;

      actualChecksum = getChecksum(storedValue, baseOffset);
      return SystemChecksum.isInvalidationChecksum(actualChecksum);
    } else {
      return false;
    }
  }

  public static boolean isCompressed(byte[] storedValue, int baseOffset) {
    return getCompressedLength(storedValue, baseOffset) < getUncompressedLength(storedValue, baseOffset);
  }

  public static int getCompressedLength(byte[] storedValue, int baseOffset) {
    int checksumLength;

    checksumLength = getChecksumLength(storedValue, baseOffset);
    return getStoredLength(storedValue, baseOffset) - checksumLength - getUserDataLength(storedValue,
        baseOffset) - fixedMetaDataLength;
  }

  public static int getStoredLength(byte[] storedValue, int baseOffset) {
    return NumConversion.bytesToInt(storedValue, baseOffset + storedLengthOffset);
  }

  public static int getUncompressedLength(byte[] storedValue, int baseOffset) {
    return NumConversion.bytesToInt(storedValue, baseOffset + uncompressedLengthOffset);
  }

  public static long getVersion(ByteBuffer buf, int baseOffset) {
    return buf.getLong(baseOffset + versionOffset);
  }

  public static long getVersion(byte[] storedValue, int baseOffset) {
    return NumConversion.bytesToLong(storedValue, baseOffset + versionOffset);
  }

  public static int getDataOffset(ChecksumType checksumType) {
    return dataOffset + checksumType.length();
  }

  public static long getCreationTime(ByteBuffer buf, int baseOffset) {
    return buf.getLong(baseOffset + creationTimeOffset);
  }

  public static long getCreationTime(byte[] storedValue, int baseOffset) {
    return NumConversion.bytesToLong(storedValue, baseOffset + creationTimeOffset);
  }

  public static ValueCreator getCreator(byte[] storedValue, int baseOffset) {
    return new SimpleValueCreator(storedValue, baseOffset + creatorOffset);
  }

  public static short getLockSeconds(ByteBuffer buf, int baseOffset) {
    return buf.getShort(baseOffset + lockSecondsOffset);
  }

  public static short getLockSeconds(byte[] storedValue, int baseOffset) {
    return NumConversion.bytesToShort(storedValue, baseOffset + lockSecondsOffset);
  }

  public static boolean isSegmented(byte[] storedValue, int baseOffset) {
    return ArrayUtil.equals(storedValue, baseOffset + creatorOffset, MetaDataConstants.segmentationBytes, 0,
        MetaDataConstants.segmentationBytes.length);
  }

  public static boolean isSegmented(ByteBuffer buf) {
    // segmentation is indicated by segmentationBytes stored in the creator field
    return BufferUtil.equals(buf, creatorOffset, MetaDataConstants.segmentationBytes, 0,
        MetaDataConstants.segmentationBytes.length);
  }

  public static int getDataOffset(byte[] storedValue, int baseOffset) {
    return baseOffset + dataOffset + getChecksumLength(storedValue, baseOffset);
  }

  public static int getDataOffset(ByteBuffer storedValue, int baseOffset) {
    return baseOffset + dataOffset + getChecksumLength(storedValue, baseOffset);
  }

  private static int getUserDataOffset(byte[] storedValue, int baseOffset) {
    return dataOffset + getStoredLength(storedValue, baseOffset) + getChecksumLength(storedValue, baseOffset);
  }

  public static int getUserDataLength(byte[] storedValue, int baseOffset) {
    return NumConversion.unsignedByteToInt(storedValue, baseOffset + userDataLengthOffset);
  }

  public static byte[] getUserData(byte[] storedValue, int baseOffset) {
    int userDataLength;
    byte[] userData;

    userDataLength = getUserDataLength(storedValue, baseOffset);
    userData = new byte[userDataLength];
    if (userDataLength > 0) {
      System.arraycopy(storedValue, baseOffset + getUserDataOffset(storedValue, baseOffset), userData, 0,
          userDataLength);
      return userData;
    } else {
      return null;
    }
  }

  public static int getMetaDataLength(byte[] storedValue, int baseOffset) {
    // FIXME - this is ignoring user data
    // current plan is to copy userdata when it is present
    return fixedMetaDataLength + getChecksumLength(storedValue, baseOffset);
  }

  // compression is moved into perspective for now
  //public static Compression getCompression(byte[] storedValue, int baseOffset) {
  //    return Compression.values()[storedValue[baseOffset + compressionTypeOffset]];
  //}

  ////////////////////////////////////////////
  // ByteBuffer versions

  public static int getCompressedLength(ByteBuffer storedValue, int baseOffset) {
    int checksumLength;

    checksumLength = getChecksumLength(storedValue, baseOffset);
    return getStoredLength(storedValue, baseOffset) - checksumLength // property of namespace
        - getUserDataLength(storedValue, baseOffset) - fixedMetaDataLength;
  }

  public static int getUncompressedLength(ByteBuffer storedValue, int baseOffset) {
    return storedValue.getInt(baseOffset + uncompressedLengthOffset);
  }

  private static int getChecksumLength(ByteBuffer storedValue, int baseOffset) {
    ChecksumType checksumType;

    checksumType = getChecksumType(storedValue, baseOffset);
    return checksumType.length();
  }

  private static int getUserDataOffset(ByteBuffer storedValue, int baseOffset) {
    return dataOffset + getStoredLength(storedValue, baseOffset) + getChecksumLength(storedValue, baseOffset);
  }

  public static int getUserDataLength(ByteBuffer storedValue, int baseOffset) {
    return NumConversion.unsignedByteToInt(storedValue.get(baseOffset + userDataLengthOffset));
  }

  public static byte[] getUserData(ByteBuffer storedValue, int baseOffset) {
    int userDataLength;
    byte[] userData;

    userDataLength = getUserDataLength(storedValue, baseOffset);
    userData = new byte[userDataLength];
    if (userDataLength > 0) {
      storedValue.get(userData, baseOffset + getUserDataOffset(storedValue, baseOffset), userDataLength);
      return userData;
    } else {
      return null;
    }
  }

  public static byte getCompression(ByteBuffer storedValue, int baseOffset) {
    return CCSSUtil.getCompression(getCCSS(storedValue, baseOffset));
  }

  public static ChecksumType getChecksumType(ByteBuffer storedValue, int baseOffset) {
    return CCSSUtil.getChecksumType(getCCSS(storedValue, baseOffset));
  }

  public static boolean isInvalidation(ByteBuffer storedValue, int baseOffset) {
    ChecksumType checksumType;

    checksumType = getChecksumType(storedValue, baseOffset);
    if (checksumType == ChecksumType.SYSTEM) {
      byte[] actualChecksum;

      actualChecksum = getChecksum(storedValue, baseOffset);
      return SystemChecksum.isInvalidationChecksum(actualChecksum);
    } else {
      return false;
    }
  }

  public static ValueCreator getCreator(ByteBuffer storedValue, int baseOffset) {
    return new SimpleValueCreator(storedValue, baseOffset + creatorOffset);
  }

  public static byte getStorageState(ByteBuffer storedValue, int baseOffset) {
    return CCSSUtil.getStorageState(getCCSS(storedValue, baseOffset));
  }

  public static short getCCSS(ByteBuffer storedValue, int baseOffset) {
    //System.out.println(storedValue +"\t"+ baseOffset +"\t"+ compressionAndChecksumOffset);
    return storedValue.getShort(baseOffset + ccss);
  }

  public static int getStoredLength(ByteBuffer storedValue, int baseOffset) {
    //System.out.println("\tgetStoredLength\t"+ storedValue +"\t"+ baseOffset +"\t"+ storedLengthOffset +"\t"+
    // storedValue.getInt(baseOffset + storedLengthOffset));
    return storedValue.getInt(baseOffset + storedLengthOffset);
  }

  public static int getMetaDataLength(ByteBuffer storedValue, int baseOffset) {
    // FIXME - this is ignoring user data
    // current plan is to copy userdata when it is present
    return fixedMetaDataLength + getChecksumLength(storedValue, baseOffset);
  }

  /////////////////

  public static void updateStorageState(ByteBuffer storedValue, int baseOffset, byte storageState) {
    int offset;
    int oldCCSS;

    offset = baseOffset + ccss;
    oldCCSS = storedValue.getShort(offset);
    //System.out.println("oldCCSS: "+ oldCCSS);
    storedValue.putShort(offset, CCSSUtil.updateStorageState(oldCCSS, storageState));
    //System.out.println("newCCSS: "+ storedValue.get(offset));
  }

  public static void testCorruption(ByteBuffer value, double corruptionProbability, int index) {
    if (ThreadLocalRandom.current().nextDouble() < corruptionProbability) {
      if (value != null && value.limit() > index) {
        System.out.printf("Corrupting %d\n", index);
        System.out.printf("   %s\n", StringUtil.byteBufferToHexString(value));
        value.put(index, (byte) (value.get(index) + 1));
        System.out.printf("=> %s\n", StringUtil.byteBufferToHexString(value));
      }
    }
  }
}

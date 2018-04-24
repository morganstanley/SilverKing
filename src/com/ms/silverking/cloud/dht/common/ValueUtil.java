package com.ms.silverking.cloud.dht.common;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.impl.Checksum;
import com.ms.silverking.cloud.dht.client.impl.ChecksumProvider;
import com.ms.silverking.compression.CodecProvider;
import com.ms.silverking.compression.Decompressor;
import com.ms.silverking.log.Log;
import com.ms.silverking.text.StringUtil;

public class ValueUtil {
    private static final boolean    debugChecksum = false;
    
    public static final ByteBuffer  corruptValue = ByteBuffer.allocate(0);
    
    public static void verifyChecksum(ByteBuffer storedValue) throws CorruptValueException {
        if (storedValue != null) {
            int    baseOffset;
            byte[] storedData;
            byte[] dataToVerify;
            int    verifyDataOffset;
            int    dataOffset;
            int    compressedLength;
            int    uncompressedLength;
            Compression compression;
            
            storedValue = storedValue.duplicate(); // don't alter the source buffer
            baseOffset = storedValue.position();
            storedData = new byte[storedValue.limit()];
            storedValue.get(storedData);
            compressedLength = MetaDataUtil.getCompressedLength(storedData, baseOffset);
            uncompressedLength = MetaDataUtil.getUncompressedLength(storedData, baseOffset);
            if (debugChecksum) {
                System.out.println("compressedLength: "+ compressedLength);
                System.out.println("uncompressedLength: "+ uncompressedLength);
            }
            dataOffset = MetaDataUtil.getDataOffset(storedData, baseOffset);
            compression = EnumValues.compression[MetaDataUtil.getCompression(storedData, baseOffset)];
            if (MetaDataUtil.isCompressed(storedData, baseOffset)) {
                byte[]          uncompressedData;
                Decompressor    decompressor;
                
                Log.fine("Compressed");
                decompressor = CodecProvider.getDecompressor(compression);
                try {
                    //System.out.println(compression +" "+ decompressor);
                    uncompressedData = decompressor.decompress(storedData, dataOffset, compressedLength, uncompressedLength);
                    dataToVerify = uncompressedData;
                    verifyDataOffset = 0;
                } catch (Exception e) {
                    throw new CorruptValueException(e);
                }
            } else {
                dataToVerify = storedData;
                verifyDataOffset = dataOffset;
            }
            verifyChecksum(storedData, baseOffset, dataToVerify, verifyDataOffset, uncompressedLength);
            Log.warningAsync("Checksum OK");
        }
    }
    
    public static boolean isInvalidated(byte[] storedValue, int storedOffset) {
    	return MetaDataUtil.isInvalidated(storedValue, storedOffset);
    }
    
    public static void verifyChecksum(byte[] storedValue, int storedOffset, byte[] value, int valueOffset,
            int valueLength) throws CorruptValueException {
        byte[] expectedChecksum;
        byte[] actualChecksum;
        ChecksumType checksumType;
        Checksum checksum;

        if (debugChecksum) {
            System.out.println("storedValue: " + StringUtil.byteArrayToHexString(storedValue));
        }
        checksumType = MetaDataUtil.getChecksumType(storedValue, storedOffset);
        if (checksumType != ChecksumType.NONE) {
            actualChecksum = MetaDataUtil.getChecksum(storedValue, storedOffset);
            checksum = ChecksumProvider.getChecksum(checksumType);
            if (!checksum.isEmpty(actualChecksum)) {
                expectedChecksum = checksum.checksum(value, valueOffset, valueLength);
                if (debugChecksum) {
                	System.out.println("valueOffset: "+ valueOffset);
                	System.out.println("valueLength: "+ valueLength);
                    System.out.println("value: " + StringUtil.byteArrayToHexString(value, valueOffset, valueLength)
                            + "\t" + valueLength);
                    System.out.println("value: " + new String(value, valueOffset, valueLength));
                    System.out.println("expectedChecksum: " + StringUtil.byteArrayToHexString(expectedChecksum));
                    System.out.println("actualChecksum: " + StringUtil.byteArrayToHexString(actualChecksum));
                    System.out.flush();
                }
                if (!Arrays.equals(expectedChecksum, actualChecksum)) {
                    throw new CorruptValueException(StringUtil.byteArrayToHexString(actualChecksum) + " != "
                            + StringUtil.byteArrayToHexString(expectedChecksum));
                }
            }
        }
    }
}

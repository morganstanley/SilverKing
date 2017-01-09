package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.client.MetaData;
import com.ms.silverking.text.StringUtil;

public class MetaDataTextUtil {
    public static String toMetaDataString(MetaData metaData, boolean labeled) {
        StringBuilder   sb;
        byte[]          userData;
        
        assert metaData != null;
        sb = new StringBuilder();
        format(sb, "StoredLength", Integer.toString(metaData.getStoredLength()), labeled);
        format(sb, "UncompressedLength", Integer.toString(metaData.getUncompressedLength()), labeled);
        format(sb, "Compression", metaData.getCompression().toString(), labeled);
        format(sb, "Version", Long.toString(metaData.getVersion()), labeled);
        format(sb, "CreationTime", metaData.getCreationTime() 
                +"\t["+ metaData.getCreationTime().toDateString() +"]", labeled);
        format(sb, "ValueCreator", metaData.getCreator().toString(), labeled);
        format(sb, "ChecksumType", metaData.getChecksumType().toString(), labeled);
        format(sb, "Checksum", StringUtil.byteArrayToHexString(metaData.getChecksum()), labeled);
        /*
        if (labeled) {
            sb.append("UserData: ");
        }
        userData = metaData.getUserData();
        if (userData == null) {
            sb.append("null");
        } else {
            sb.append(StringUtil.byteArrayToHexString(userData));
        }
        // FIXME complete user data
        */
        return sb.toString();
    }

    private static void format(StringBuilder sb, String string, String value, boolean labeled) {
        if (labeled) {
            sb.append(String.format("\n%-20s %s", string +":", value));
        } else {
            sb.append(String.format("\n%s", value));
        }
    }
}

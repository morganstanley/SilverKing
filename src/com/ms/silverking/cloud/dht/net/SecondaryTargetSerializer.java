package com.ms.silverking.cloud.dht.net;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.bytes.ByteList;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.SecondaryTarget;
import com.ms.silverking.cloud.dht.client.SecondaryTargetType;
import com.ms.silverking.log.Log;
import com.ms.silverking.numeric.NumConversion;

public class SecondaryTargetSerializer {
    
    private static final int    initialBufferSize = 64;
    
    public static int serializedLength(Set<SecondaryTarget> specs) {
        return specs == null ? 0 : serialize(specs).length;
    }

    public static byte[] serialize(Set<SecondaryTarget> specs) {
        ByteList    list;
        
        list = new ByteArrayList(initialBufferSize);
        for (SecondaryTarget spec : specs) {
            byte[]  targetBytes;
            
            list.add((byte)spec.getType().ordinal());
            targetBytes = spec.getTarget().getBytes();
            list.addElements(list.size(), NumConversion.shortToBytes((short)targetBytes.length));
            list.addElements(list.size(), targetBytes);
        }
        return list.toByteArray();
    }
    
    public static Set<SecondaryTarget> deserialize(byte[] multiDef) {
        try {
            ImmutableSet.Builder<SecondaryTarget>  specs;
            int i;
            
            specs = ImmutableSet.builder();
            i = 0;
            while (i < multiDef.length) {
                SecondaryTargetType type;
                int     targetSize;
                byte[]  targetBytes;
                String  target;
                
                type = SecondaryTargetType.values()[multiDef[i++]];
                targetSize = NumConversion.bytesToShort(multiDef, i);
                i += NumConversion.BYTES_PER_SHORT;
                targetBytes = new byte[targetSize];
                System.arraycopy(multiDef, i, targetBytes, 0, targetSize);
                i += targetSize;
                target = new String(targetBytes);
                specs.add(new SecondaryTarget(type, target));
            }
            return specs.build();
        } catch (Exception e) {
            Log.logErrorWarning(e);
            return ImmutableSet.of();
        }
    }
}

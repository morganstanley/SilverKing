package com.ms.silverking.cloud.dht.benchmark.ycsb;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.ms.silverking.cloud.dht.client.serialization.BufferSerDes;
import com.ms.silverking.numeric.NumConversion;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;

/**
 * Serializer/deserializer for byte[]. No copy of source data is made for put().
 */
public final class RecordSerDes implements BufferSerDes<Map> {
    private static final boolean    debug = false;
    
    @Override
    public ByteBuffer serializeToBuffer(Map _map) {
        byte[]  array;
        int     index;
        Map<String,ByteIterator> map;
        
        map = (Map<String,ByteIterator>)_map;
        array = new byte[estimateSerializedSize(map)];
        index = 0;
        for (Map.Entry<String, ByteIterator> entry : map.entrySet()) {
            index = addArrayWithLength(entry.getKey().getBytes(), array, index);
            index = addArrayWithLength(entry.getValue().toArray(), array, index);
        }
        return ByteBuffer.wrap(array);
    }
    
    @Override
    public void serializeToBuffer(Map _map, ByteBuffer dest) {        
        Map<String,ByteIterator> map;
        
        map = (Map<String,ByteIterator>)_map;
        for (Map.Entry<String, ByteIterator> entry : map.entrySet()) {
            addArrayWithLength(entry.getKey().getBytes(), dest);
            addArrayWithLength(entry.getValue().toArray(), dest);
        }
    }
    
    private int addArrayWithLength(byte[] source, byte[] dest, int index) {
        NumConversion.intToBytes(source.length, dest, index);
        index += NumConversion.BYTES_PER_INT;
        System.arraycopy(source, 0, dest, index, source.length);
        index += source.length;
        return index;
    }
    
    private void addArrayWithLength(byte[] source, ByteBuffer buf) {
        buf.putInt(source.length);
        buf.put(source);
    }

    @Override
    public int estimateSerializedSize(Map _map) {
        int size;
        Map<String,ByteIterator> map;
        
        map = (Map<String,ByteIterator>)_map;
        size = 0;
        for (Map.Entry<String, ByteIterator> entry : map.entrySet()) {
            size += entry.getKey().length() + entry.getValue().bytesLeft() + (2 * NumConversion.BYTES_PER_INT);
            //byte[]  array;
            
            //array = entry.getValue().toArray();
            //System.out.println(entry.getKey() +"\t"+ entry.getValue().bytesLeft());
            //size += entry.getKey().length() + array.length + (2 * NumConversion.BYTES_PER_INT);
        }
        return size;
    }

    @Override
    public Map<String,ByteIterator> deserialize(ByteBuffer[] buffers) {
        throw new RuntimeException("Not yet implemented");
    }

    @Override
    public Map<String,ByteIterator> deserialize(ByteBuffer buf) {
        Map<String,ByteIterator>  map;
        
        buf = buf.asReadOnlyBuffer();
        if (debug) {
            System.out.println("\n\ndeserialize **** "+ buf);
        }
        map = new HashMap<>();
        while (buf.hasRemaining()) {
            if (debug) {
                System.out.println("\n"+ buf);
            }
            map.put(new String(readArray(buf)), new ByteArrayByteIterator(readArray(buf)));
        }
        return map;
    }
    
    private byte[] readArray(ByteBuffer buf) {
        byte[]  array;

        array = new byte[buf.getInt()];
        if (debug) {
            System.out.println(array.length);
        }
        buf.get(array);
        return array;
    }

	@Override
	public Map emptyObject() {
		return ImmutableMap.of();
	}
}

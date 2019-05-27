package com.ms.silverking.cloud.dht.benchmark.ycsb;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.client.serialization.BufferSerDes;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.yahoo.ycsb.ByteIterator;

/**
 * Serializer/deserializer for byte[]. No copy of source data is made for put().
 */
public final class ByteIteratorSerDes implements BufferSerDes<ByteIterator> {
    @Override
    public ByteBuffer serializeToBuffer(ByteIterator bi) {
        return ByteBuffer.wrap(bi.toArray());
    }

    @Override
    public void serializeToBuffer(ByteIterator bi, ByteBuffer buffer) {
        buffer.put(bi.toArray());
    }

    @Override
    public int estimateSerializedSize(ByteIterator bi) {
        return (int)bi.bytesLeft();
    }

    @Override
    public ByteIterator deserialize(ByteBuffer[] buffers) {
        throw new RuntimeException("Not yet implemented");
        /*
        if (buffers.length != 0) {
            byte[]     bytes;
            ByteBuffer buffer;
            
            buffer = buffers[0];
            bytes = buffer.array();
            return new String(bytes, buffer.position(), buffer.remaining());
        } else {
            throw new RuntimeException("Deserialization error");
        }
        */
    }

    @Override
    public ByteIterator deserialize(ByteBuffer buffer) {
        //return Arrays.copyOfRange(buffer.array(), buffer.position(), buffer.position() + buffer.remaining());
        return new ByteBufferByteIterator(buffer);
    }
    
    static class ByteBufferByteIterator extends ByteIterator {
        private final ByteBuffer    buf;
        
        public ByteBufferByteIterator(ByteBuffer buf) {
            this.buf = buf;
        }
        
        @Override
        public long bytesLeft() {
            return buf.remaining();
        }

        @Override
        public boolean hasNext() {
            return buf.hasRemaining();
        }

        @Override
        public byte nextByte() {
            return (byte)buf.get();
        }
    }

	@Override
	public ByteIterator emptyObject() {
		return new ByteBufferByteIterator(ByteBuffer.wrap(DHTConstants.emptyByteArray));
	}
                                            
                                            /*
	@Override
	public byte[] serialize(byte[] bytes) {
		return bytes;
	}
	
	@Override 
	public ByteBuffer serializeToBuffer(byte[] bytes) {
	    return ByteBuffer.wrap(bytes);
	}

	@Override
	public byte[] deserialize(ByteBuffer[] buffers) {
        if (buffers.length == 0) {
            byte[]      srcBytes;
            byte[]      destBytes;
            ByteBuffer  buffer;
            int         length;
            
            buffer = buffers[0];
            srcBytes = buffer.array();
            destBytes = new byte[buffer.remaining()];
            System.arraycopy(srcBytes, buffer.position(), destBytes, 0, destBytes.length);
            return destBytes;
        } else {
            throw new RuntimeException("Deserialization error");
        }
		// FUTURE - Could let users decide how to handle checksum copy 
		//return bytes;
	}
	*/
}

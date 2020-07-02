package com.ms.silverking.cloud.dht.net;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.net.protocol.KeyValueMessageFormat;
import com.ms.silverking.id.UUIDBase;

/**
 * ProtoMessageGroup for messages that contains values e.g. put and retrieval response.
 * Values are stored in a list of ByteBuffers. Smaller values may be copied into a shared ByteBuffer.
 * Larger values are stored in dedicated ByteBuffers, possibly by creating a zero-copy ByteBuffer
 * wrapping existing storage of the value.
 */
abstract class ProtoValueMessageGroupBase extends ProtoKeyedMessageGroup {
  protected final int opSize;
  protected ByteBuffer valueBuffer;
  protected int curMultiValueBufferIndex;
  protected int totalValueBytes;

  // FIXME - valueBufferSize should be computed based on some
  // external information
  protected static final int valueBufferSize = 16 * 1024;
  // Maximum number of value bytes to allow in a single message. The sum of the length of all values in
  // a message must be less than this limit. Operations that need to transfer more bytes over the
  // network will use multiple messages to do so.
  // maxValueSegmentSize is used below, because this needs to be at least that, but
  // we could consider higher and make that a minimum.
  public static final int maxValueBytesPerMessage = DHTConstants.defaultFragmentationThreshold + 1024 * 1024;
  private static final int extraMargin = 1024;
  // Values below this size are copied into a common buffer. Values above this size are contained in dedicated
  // buffers, possibly with zero-copy
  protected static final int dedicatedBufferSizeThreshold = 16 * 1024;

  static {
    // Values without dedicated buffers need to be able to fit into the shared buffer
    if (dedicatedBufferSizeThreshold > valueBufferSize) {
      throw new RuntimeException("dedicatedBufferSizeThreshold > valueBufferSize not supported");
    }
  }

  public ProtoValueMessageGroupBase(MessageType type, UUIDBase uuid, long context, int opSize, int valueBytes,
      ByteBuffer optionsByteBuffer, int additionalBytesPerKey, byte[] originator, int deadlineRelativeMillis,
      ForwardingMode forward, byte[] maybeTraceID) {
    super(type, uuid, context, optionsByteBuffer, opSize, additionalBytesPerKey, originator, deadlineRelativeMillis,
        forward, maybeTraceID);
    this.opSize = opSize;
    bufferList.add(optionsByteBuffer);
        /*
        if (opSize > 1) {
            if (valueBytes > 0) {
                //valueBuffer = ByteBuffer.allocate(dedicatedBufferSizeThreshold);
                valueBuffer = ByteBuffer.allocate(valueBytes);
                // FIXME - above may be switched to allocate direct
                // However, that requires a careful think through all copying
                // and direct buffer usage
                //curMultiValueBufferIndex = (short)bufferList.size();
                //bufferList.add(valueBuffer);
                addMultiValueBuffer(valueBuffer);
            }
        } else {
            valueBuffer = null;
            curMultiValueBufferIndex = -1;
        }
        */
    valueBuffer = null;
    curMultiValueBufferIndex = -1;
  }

  static int getOptionsByteBufferBaseOffset(boolean hasTraceID) {
    // No extra thing is added at this layer, so baseOffset = 0 + previousLayerOffset
    return 0 + ProtoKeyedMessageGroup.getOptionsByteBufferBaseOffset(hasTraceID);
  }

  public boolean canBeAdded(int valueSize) {
    // Check that we are below maxValueBytesPerMessage
    // Exception is granted for single values of any length
    // (If fragmentation threshold is very high, we need to handle very large values)
    return (totalValueBytes == 0) || (totalValueBytes + valueSize < maxValueBytesPerMessage);
  }

  public int currentValueBytes() {
    return totalValueBytes;
  }

  protected boolean addDedicatedBuffer(ByteBuffer buffer) {
    if (bufferList.size() >= Integer.MAX_VALUE) {
      return false;
    } else {
      bufferList.add(buffer);
      return true;
    }
  }

  protected boolean addMultiValueBuffer(ByteBuffer buffer) {
    if (bufferList.size() >= Integer.MAX_VALUE) {
      return false;
    } else {
      curMultiValueBufferIndex = bufferList.size();
      bufferList.add(buffer);
      return true;
    }
  }

  public void addErrorCode(DHTKey key) {
    addKey(key);
    keyByteBuffer.putInt(-1);
    keyByteBuffer.putInt(-1);
    keyByteBuffer.putInt(-1);
  }
}

package com.ms.silverking.cloud.dht.net;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Optional;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.net.protocol.KeyedMessageFormat;
import com.ms.silverking.cloud.dht.trace.TraceIDProvider;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.numeric.NumConversion;

/**
 * ProtoMessageGroup for keyed messages
 */
public abstract class ProtoKeyedMessageGroup extends ProtoMessageGroup {
  private final int keyBufferAdditionalBytesPerKey;
  protected ByteBuffer keyByteBuffer;
  protected final ByteBuffer optionsByteBuffer;
  private int totalKeys;
  protected final boolean hasTraceID;

  private static final int keyBufferExpansionKeys = 32;
  protected static final int keyByteBufferIndex = 0;
  protected static final int traceIDBufferListIndex = 1;

  public ProtoKeyedMessageGroup(MessageType type, UUIDBase uuid, long context, ByteBuffer optionsByteBuffer,
      int numKeys, int keyBufferAdditionalBytesPerKey, byte[] originator, int deadlineRelativeMillis,
      ForwardingMode forward, byte[] maybeTraceID) {
    super(type, uuid, context, originator, deadlineRelativeMillis, forward);

    this.keyBufferAdditionalBytesPerKey = keyBufferAdditionalBytesPerKey;
    keyByteBuffer = allocateKeyBuffer(numKeys, keyBufferAdditionalBytesPerKey);

    //System.out.println("keyBufferSize: "+ keyBufferSize
    //        +"\tkeyBufferAdditionalBytesPerKey: "+ keyBufferAdditionalBytesPerKey);

    bufferList.add(keyByteBuffer); // index 0
    hasTraceID = TraceIDProvider.hasTraceID(type);
    if (TraceIDProvider.hasTraceID(type)) {
      bufferList.add(ByteBuffer.wrap(maybeTraceID)); // index 1
    }

    // bufferList can have 1 (no traceID) or 2 (have traceID)
    this.optionsByteBuffer = optionsByteBuffer;
    // added to list by children
  }

  static int getOptionsByteBufferBaseOffset(boolean hasTraceID) {
    return hasTraceID ? 2 : 1;
  }

  /**
   * NOTE: Caller of this method shall be responsible for checking return value is valid
   * (i.e. call TraceIDProvider.isValidTraceID(byte[] maybeTraceID))
   */
  public static byte[] unsafeGetTraceIDCopy(MessageGroup mg) {
    if (TraceIDProvider.hasTraceID(mg.getMessageType())) {
      ByteBuffer buffer;
      byte[] copy;
      int len;

      buffer = mg.getBuffers()[traceIDBufferListIndex];
      len = buffer.remaining();
      copy = new byte[len];
      System.arraycopy(buffer.array(), 0, copy, 0, len);
      return copy;
    } else {
      return TraceIDProvider.noTraceID;
    }
  }

  public static Optional<byte[]> tryGetTraceIDCopy(MessageGroup mg) {
    byte[] maybeTraceID;

    maybeTraceID = unsafeGetTraceIDCopy(mg);
    if (TraceIDProvider.isValidTraceID(maybeTraceID)) {
      return Optional.of(maybeTraceID);
    } else {
      return Optional.empty();
    }
  }

  private static final ByteBuffer allocateKeyBuffer(int numKeys, int keyBufferAdditionalBytesPerKey) {
    ByteBuffer keyBuffer;
    int bytesPerEntry;

    bytesPerEntry = KeyedMessageFormat.baseBytesPerKeyEntry + keyBufferAdditionalBytesPerKey;
    keyBuffer = ByteBuffer.allocate(NumConversion.BYTES_PER_SHORT + bytesPerEntry * numKeys);
    keyBuffer.putShort((short) bytesPerEntry);
    return keyBuffer;
  }

  public int currentBufferKeys() {
    return totalKeys;
  }

  public void addKey(DHTKey dhtKey) {
    try {
      ++totalKeys;
      keyByteBuffer.putLong(dhtKey.getMSL());
      keyByteBuffer.putLong(dhtKey.getLSL());
    } catch (BufferOverflowException bfe) {
      ByteBuffer newKeyByteBuffer;

      Log.fine("ProtoKeyedMessageGroup keyByteBuffer overflow. Expanding.");
      newKeyByteBuffer = allocateKeyBuffer(currentBufferKeys() + keyBufferExpansionKeys,
          keyBufferAdditionalBytesPerKey);
      keyByteBuffer.flip();
      newKeyByteBuffer.put(keyByteBuffer);
      keyByteBuffer = newKeyByteBuffer;
      keyByteBuffer.putLong(dhtKey.getMSL());
      keyByteBuffer.putLong(dhtKey.getLSL());
    }
  }

  public boolean isNonEmpty() {
    return keyByteBuffer.position() != 0;
  }

  // Used before some subclasses force to do flip(), which get traceID wiped
  protected void tryConsumeTraceID() {
    if (hasTraceID) {
      ByteBuffer traceID;

      traceID = bufferList.get(traceIDBufferListIndex);
      if (traceID.position() != traceID.limit()) {
        traceID.position(traceID.limit());
      }
    }
  }

  @Override
  public MessageGroup toMessageGroup() {
    return this.toMessageGroup(true);
  }

  @Override
  protected MessageGroup toMessageGroup(boolean flip) {
    if (hasTraceID) {
      ByteBuffer traceID;

      traceID = bufferList.get(traceIDBufferListIndex);
      // Make sure traceID is not wiped after flip
      if (flip) {
        tryConsumeTraceID();
      } else {
        if (traceID.position() != 0) {
          traceID.position(0);
        }
      }
      return super.toMessageGroup(flip);
    } else {
      return super.toMessageGroup(flip);
    }
  }
}

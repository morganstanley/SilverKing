package com.ms.silverking.cloud.dht.daemon.storage.fsm;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.log.Log;
import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.text.StringUtil;

public class LTVElement {
  protected final ByteBuffer buf;

  private static final int lengthOffset = 0;
  private static final int typeOffset = lengthOffset + NumConversion.BYTES_PER_INT;
  protected static final int valueOffset = typeOffset + NumConversion.BYTES_PER_INT;
  private static final int headerSizeBytes = valueOffset;

  protected static final boolean debug = false;

  public LTVElement(ByteBuffer buf) {
    if (debug) {
      Log.warningf("LTVElement buf %s", buf);
    }
    this.buf = buf.order(ByteOrder.nativeOrder());
  }

  public int getLength() {
    return buf.getInt(lengthOffset);
  }

  public int getType() {
    return buf.getInt(typeOffset);
  }

  public int getValueLength() {
    return getLength() - getHeaderSizeBytes();
  }

  public ByteBuffer getBuffer() {
    return BufferUtil.duplicate(buf);
  }

  public ByteBuffer getValueBuffer() {
    if (debug) {
      Log.warningf("buf %s", buf);
      Log.warningf("headerSizeBytes %s", headerSizeBytes);
      Log.warningf("getLength() %d", getLength());
    }
    return BufferUtil.sliceRange(buf, headerSizeBytes, getLength());
  }

  public int getValueOffset() {
    return valueOffset;
  }

  public static int getHeaderSizeBytes() {
    return headerSizeBytes;
  }

  @Override
  public String toString() {
    return String.format("%d %d %s", getLength(), getType(), StringUtil.byteBufferToHexString(getValueBuffer()));
  }

  /////////////////////

  public static ByteBuffer readElementBuffer(ByteBuffer buf, int offset) {
    int length;
    ByteBuffer elementBuf;

    try {
      length = buf.getInt(offset + lengthOffset);
      //System.out.printf("length %d\n", length);
      //elementBuf = BufferUtil.get(buf, offset, length);
      elementBuf = BufferUtil.sliceRange(buf, offset, offset + length);
    } catch (RuntimeException re) {
      System.out.printf("%d %d\n", offset, lengthOffset);
      System.out.printf("%s\n", buf);
      //System.out.printf("%s\n", StringUtil.byteBufferToHexString(buf));
      throw re;
    }
    return elementBuf;
  }
}

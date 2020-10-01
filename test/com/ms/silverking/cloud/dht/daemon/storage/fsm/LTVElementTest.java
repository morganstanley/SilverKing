package com.ms.silverking.cloud.dht.daemon.storage.fsm;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.ms.silverking.numeric.NumConversion;
import org.junit.Assert;
import org.junit.Test;

public class LTVElementTest {
  @Test
  public void ltvTest() {
    ByteBuffer b;
    LTVElement e;
    int length;
    int type;
    int value;

    length = NumConversion.BYTES_PER_INT * 3;
    type = 1;
    value = 0x01020304;

    b = ByteBuffer.allocate(12);
    b = b.order(ByteOrder.nativeOrder());
    b.putInt(length);
    b.putInt(type);
    b.putInt(value);

    e = new LTVElement(b);
    Assert.assertEquals(length, e.getLength());
    Assert.assertEquals(type, e.getType());
    Assert.assertEquals(value, e.getValueBuffer().getInt());
  }
}

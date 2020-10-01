package com.ms.silverking.cloud.dht.daemon.storage.fsm;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.ms.silverking.text.StringUtil;

public class BufferTest {
  public static void main(String[] args) {
    ByteBuffer buf;

    buf = ByteBuffer.allocate(8);
    buf.putInt(0, 0x12345678);
    System.out.println("Default");
    System.out.printf("%x\n", buf.getInt(0));
    System.out.printf("\t%s\n", StringUtil.byteBufferToHexString(buf));

    buf = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder());
    buf.putInt(0, 0x12345678);
    System.out.println("nativeOrder");
    System.out.printf("%x\n", buf.getInt(0));
    System.out.printf("\t%s\n", StringUtil.byteBufferToHexString(buf));

    buf = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder());
    buf.putInt(0, 0x12345678);
    buf = buf.duplicate();
    System.out.println("nativeOrder put, duplicate read");
    System.out.printf("%x\n", buf.getInt(0));
    System.out.printf("\t%s\n", StringUtil.byteBufferToHexString(buf));
    buf = buf.duplicate().slice().order(ByteOrder.BIG_ENDIAN);
    System.out.println("nativeOrder put, duplicate.slice read");
    System.out.printf("%x\n", buf.getInt(0));
    System.out.printf("\t%s\n", StringUtil.byteBufferToHexString(buf));
  }
}

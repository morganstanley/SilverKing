package com.ms.silverking.cloud.dht.daemon.storage.fsm;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.Test;

public class InvalidatedOffsetsElementTest {
  private static final int numOffsets = 10;

  @Test
  public void test() {
    Set<Integer> io0;
    Set<Integer> io1;
    InvalidatedOffsetsElement e;

    io0 = new HashSet<>();
    for (int i = 0; i < numOffsets; i++) {
      io0.add(ThreadLocalRandom.current().nextInt());
    }

    e = InvalidatedOffsetsElement.create(io0);
    io1 = e.getInvalidatedOffsets();
    //System.out.printf("%s\n", StringUtil.toString(io0, ' '));
    //System.out.printf("%s\n", StringUtil.toString(io1, ' '));
    System.out.printf("%s\n", io0.equals(io1));
    Assert.assertEquals(io0, io1);
  }
}

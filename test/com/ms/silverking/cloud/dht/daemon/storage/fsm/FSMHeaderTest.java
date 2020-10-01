package com.ms.silverking.cloud.dht.daemon.storage.fsm;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class FSMHeaderTest {
  @Test
  public void fsmHeaderTest() {
    Map<FSMElementType, Integer> entries;
    int offset;
    FSMHeader h;
    FSMHeader h2;
    FSMHeaderElement e;

    offset = 0;
    entries = new HashMap<>();
    for (FSMElementType type : FSMElementType.values()) {
      entries.put(type, offset);
      offset += 128;
    }

    h = new FSMHeader(entries);
    System.out.printf("%s\n", h);

    e = FSMHeaderElement.createFromHeader(h);
    h2 = e.toFSMHeader();

    System.out.println();
    System.out.printf("%s\n", h2);

    Assert.assertEquals(h.toString(), h2.toString());
  }
}

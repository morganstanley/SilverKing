package com.ms.silverking.cloud.dht.meta;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class UtilTest {

  @Test
  public void testGetFreePort() {
    int port = Util.getFreePort();
    assertTrue(port >= Util.portMin);
    assertTrue(port <= Util.portMax);
  }

  // FUTURE:bph: hard to reliably test
  //    @Test
  //    public void testIsFree() {
  //        assertTrue( StaticDHTCreator.isFree(1_000_000));
  //        assertFalse(StaticDHTCreator.isFree(22));
  //    }

}

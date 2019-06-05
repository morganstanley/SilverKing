package com.ms.silverking.cloud.dht;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.client.SecondaryTargetType;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;
import com.ms.silverking.cloud.dht.net.ForwardingMode;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoRetrievalMessageGroup;
import com.ms.silverking.cloud.dht.net.protocol.RetrievalMessageFormat;
import com.ms.silverking.id.UUIDBase;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Set;

public class ArbitraryUserRetrievalOptionsTest {

  private void assertUserOptions(byte[] testUserOpts, String expectedStr, byte[] expectedBytes) {

    assertNotNull("rebuilt options should not be null", testUserOpts);
    assertTrue("rebuilt options should not be empty", testUserOpts.length > 0);

    String rebuiltStr = new String(testUserOpts);
    assertEquals("rebuilt user options should match original string", expectedStr, rebuiltStr);

    assertArrayEquals("rebuilt user options should match original bytes", expectedBytes, testUserOpts);
  }

  @Test public void testUserOptionsPreserved() {
    String usrStr = "dog,123";
    byte[] usrBytes = usrStr.getBytes();
    GetOptions opts = DHTConstants.standardGetOptions.userOptions(usrBytes);

    InternalRetrievalOptions internalOpts = new InternalRetrievalOptions(opts);

    byte[] originator = {0, 1, 2, 3, 4, 5, 6, 7};

    ProtoRetrievalMessageGroup message = new ProtoRetrievalMessageGroup(
        UUIDBase.random(),
        1L,
        internalOpts,
        originator,
        1,
        MessageGroup.minDeadlineRelativeMillis,
        ForwardingMode.DO_NOT_FORWARD);

    RetrievalOptions rebuiltOpts = ProtoRetrievalMessageGroup
        .getRetrievalOptions(message.toMessageGroup())
        .getRetrievalOptions();

    int optsLength = RetrievalMessageFormat.getOptionsBufferLength(rebuiltOpts);
    int expectedLength = RetrievalMessageFormat.getOptionsBufferLength(opts);
    assertEquals("options length should be preserved", expectedLength, optsLength);

    byte[] rebuiltUserOpts = rebuiltOpts.getUserOptions();
    assertUserOptions(rebuiltUserOpts, usrStr, usrBytes);
  }

  @Test public void testUserOptionsAndSecondaryTargetsPreserved() {
    String usrStr = "dog,123";
    byte[] usrBytes = usrStr.getBytes();

    SecondaryTarget sta = new SecondaryTarget(SecondaryTargetType.NodeID, "foo");
    SecondaryTarget stb = new SecondaryTarget(SecondaryTargetType.NodeID, "bar");
    Set<SecondaryTarget> secondaryTargets = ImmutableSet.of(sta, stb);

    GetOptions opts = DHTConstants.standardGetOptions.userOptions(usrBytes).secondaryTargets(secondaryTargets);
    InternalRetrievalOptions internalOpts = new InternalRetrievalOptions(opts);

    byte[] originator = {0, 1, 2, 3, 4, 5, 6, 7};
    ProtoRetrievalMessageGroup message = new ProtoRetrievalMessageGroup(
        UUIDBase.random(),
        1L,
        internalOpts,
        originator,
        1,
        MessageGroup.minDeadlineRelativeMillis,
        ForwardingMode.DO_NOT_FORWARD);

    RetrievalOptions rebuiltOpts = ProtoRetrievalMessageGroup
        .getRetrievalOptions(message.toMessageGroup())
        .getRetrievalOptions();

    byte[] rebuiltUserOpts = rebuiltOpts.getUserOptions();

    int optsLength = RetrievalMessageFormat.getOptionsBufferLength(rebuiltOpts);
    int expectedLength = RetrievalMessageFormat.getOptionsBufferLength(opts);
    assertEquals("options length should be preserved", expectedLength, optsLength);

    assertUserOptions(rebuiltUserOpts, usrStr, usrBytes);

    Set<SecondaryTarget> rebuiltSecondaries = rebuiltOpts.getSecondaryTargets();
    assertEquals("Rebuilt secondaries and original should match", rebuiltSecondaries, secondaryTargets);
  }

  @Test public void testNullUserOptionsPreserved() {
    GetOptions opts = DHTConstants.standardGetOptions;
    assertNull("Standard options should have null user options", opts.getUserOptions());

    InternalRetrievalOptions internalOpts = new InternalRetrievalOptions(opts);

    byte[] originator = {0, 1, 2, 3, 4, 5, 6, 7};
    ProtoRetrievalMessageGroup message = new ProtoRetrievalMessageGroup(
        UUIDBase.random(),
        1L,
        internalOpts,
        originator,
        1,
        MessageGroup.minDeadlineRelativeMillis,
        ForwardingMode.DO_NOT_FORWARD);

    RetrievalOptions rebuiltOpts = ProtoRetrievalMessageGroup
        .getRetrievalOptions(message.toMessageGroup())
        .getRetrievalOptions();

    assertNull("Rebuilt options should still be null", rebuiltOpts.getUserOptions());

    int optsLength = RetrievalMessageFormat.getOptionsBufferLength(rebuiltOpts);
    int expectedLength = RetrievalMessageFormat.getOptionsBufferLength(opts);
     assertEquals("options length should be preserved", expectedLength, optsLength);

  }

}
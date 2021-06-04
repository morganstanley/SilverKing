package com.ms.silverking.cloud.dht.client.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;

import com.ms.silverking.cloud.dht.SessionPolicyOnDisconnect;
import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.client.SessionEstablishmentTimeoutController;
import com.ms.silverking.cloud.dht.client.serialization.SerializationRegistry;
import com.ms.silverking.cloud.dht.common.NamespaceOptionsMode;
import com.ms.silverking.cloud.dht.net.IPAliasMap;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.net.AddrAndPort;
import com.ms.silverking.net.security.AuthFailedException;
import com.ms.silverking.net.security.NonRetryableAuthFailedException;
import com.ms.silverking.time.AbsMillisTimeSource;
import org.junit.Test;
import org.mockito.Mockito;

public class DHTSessionImplTest {

  @Test
  public void testCheckForTimeouts() {
    long timestamp = 1546300800000L;
    int namespaceCount = 2;

    DHTSessionImpl session = mock(DHTSessionImpl.class);
    doCallRealMethod().when(session).checkForTimeouts();
    when(session.getAbsMillisTimeSource()).thenReturn(mock(AbsMillisTimeSource.class));
    when(session.getAbsMillisTimeSource().absTimeMillis()).thenReturn(timestamp);
    List<ClientNamespace> clientNamespaceList = new ArrayList<>();
    ClientNamespace namespace = mock(ClientNamespace.class);
    for (int i = 0; i < namespaceCount; i++) {
      clientNamespaceList.add(namespace);
    }
    when(session.getClientNamespaceList()).thenReturn(clientNamespaceList);
    when(session.exclusionSetHasChanged()).thenReturn(false);
    doNothing().when(namespace).checkForTimeouts(timestamp, false);

    session.checkForTimeouts();
    verify(session, times(1)).exclusionSetHasChanged();
    verify(session, times(1)).getClientNamespaceList();
    verify(namespace, times(namespaceCount)).checkForTimeouts(timestamp, false);
  }

  @Test
  public void exclusionSetHasChanged() {
    ExclusionSet exclusionSetMock = mock(ExclusionSet.class);
    DHTSessionImpl session = mock(DHTSessionImpl.class);
    when(session.getExclusionSet()).thenReturn(exclusionSetMock);
    doCallRealMethod().when(session).setExclusionSet(any(ExclusionSet.class));
    when(session.getCurrentExclusionSet()).thenReturn(exclusionSetMock);
    doCallRealMethod().when(session).exclusionSetHasChanged();
    assertFalse(session.exclusionSetHasChanged());

    ExclusionSet exclusionSetMock2 = mock(ExclusionSet.class);
    when(session.getCurrentExclusionSet()).thenReturn(exclusionSetMock2);
    assertTrue(session.exclusionSetHasChanged());

    when(session.getCurrentExclusionSet()).thenReturn(null);
    assertTrue(session.exclusionSetHasChanged());
  }

  private DHTSessionImpl getMockedClientSessionImpl(ClientDHTConfiguration dhtConfig, AddrAndPort server,
      AbsMillisTimeSource absMillisTimeSource, SerializationRegistry serializationRegistry,
      SessionEstablishmentTimeoutController timeoutController, NamespaceOptionsMode nsOptionsMode,
      boolean enableMsgGroupTrace, IPAliasMap aliasMap, SessionPolicyOnDisconnect onDisconnect,
      MessageGroupBase mockBase) throws IOException, AuthFailedException {
    return new DHTSessionImpl(dhtConfig, server, absMillisTimeSource, serializationRegistry, timeoutController,
        nsOptionsMode, enableMsgGroupTrace, aliasMap, onDisconnect) {

      @Override
      public boolean isDaemon() {
        // Daemon's don't eagerly connect
        return false;
      }

      @Override
      protected void eagerConnect() throws AuthFailedException, ConnectException {
        assert (!this.isDaemon());
        assert (this.mgBase != null);
        verify(mgBase, times(1)).enable();
        super.eagerConnect();
      }

      @Override
      protected MessageGroupBase buildMessageGroupBase(SessionEstablishmentTimeoutController timeoutController,
          IPAliasMap aliasMap, SessionPolicyOnDisconnect onDisconnect) throws IOException {
        // should call buildMessageGroupBase after assigning constructor params
        assert (absMillisTimeSource != null);
        return mockBase;
      }
    };
  }

  private void verifyTriedConnected(MessageGroupBase mgb) {
    try {
      verify(mgb, times(1)).ensureConnected(any(AddrAndPort.class));
    } catch (ConnectException | AuthFailedException e) {
      // shouldn't happen, just satisfy the signature for mock verification
      fail(e.getMessage());
    }
  }

  @Test
  public void testExceptionDuringMgBaseEnsureConnectedClosesResources() {
    ClientDHTConfiguration dhtConfig = mock(ClientDHTConfiguration.class);
    AddrAndPort server = mock(AddrAndPort.class);
    AbsMillisTimeSource absMillisTimeSource = mock(AbsMillisTimeSource.class);
    SerializationRegistry serializationRegistry = mock(SerializationRegistry.class);
    SessionEstablishmentTimeoutController timeoutController = mock(SessionEstablishmentTimeoutController.class);
    NamespaceOptionsMode nsOptionsMode = mock(NamespaceOptionsMode.class);
    IPAliasMap aliasMap = mock(IPAliasMap.class);
    SessionPolicyOnDisconnect onDisconnect = mock(SessionPolicyOnDisconnect.class);

    MessageGroupBase msgGrpBase = mock(MessageGroupBase.class);

    List<Exception> possibleExceptions = new ArrayList<>();
    possibleExceptions.add(new ConnectException("injected fault"));
    possibleExceptions.add(new NonRetryableAuthFailedException("injected fault"));
    possibleExceptions.add(new RuntimeException("injected arbitrary runtime exception types"));

    for (Exception ex : possibleExceptions) {
      try {
        Mockito.doThrow(ex).when(msgGrpBase).ensureConnected(any(AddrAndPort.class));
      } catch (AuthFailedException | ConnectException e) {
        // Shouldn't get here, we just have to satisfy the catch for the mock to occur
        fail(e.getMessage());
      }

      try {
        // Constructor will call ensureConnected
        DHTSessionImpl session = getMockedClientSessionImpl(dhtConfig, server, absMillisTimeSource,
            serializationRegistry, timeoutController, nsOptionsMode, false, aliasMap, onDisconnect, msgGrpBase);
      } catch (Exception e) {
        verify(msgGrpBase, times(1)).enable();
        verifyTriedConnected(msgGrpBase);
        // Must have tidied up on the way out
        verify(msgGrpBase, times(1)).shutdown();
      }

      Mockito.reset(msgGrpBase);
    }
  }
}

package com.ms.silverking.net.async;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import com.ms.silverking.cloud.dht.daemon.DisconnectAllExclusionResponder;
import com.ms.silverking.cloud.dht.daemon.SelfExclusionResponder;
import com.ms.silverking.cloud.dht.net.MessageGroupConnection;
import com.ms.silverking.net.IPAndPort;

import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.net.AddrAndPort;
import com.ms.silverking.net.HostAndPort;
import com.ms.silverking.net.security.AuthFailedException;
import com.ms.silverking.net.security.RetryableAuthFailedException;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class AsyncBaseConnectionTest {
  private AddressStatusProvider mockAddressStatusProvider = mock(AddressStatusProvider.class);
  private SuspectAddressListener mockSuspectAddressListener = mock(SuspectAddressListener.class);
  private AsyncServer mockAsyncServer = mock(AsyncServer.class);
  private boolean connectionStarted;
  private long realtimeDeadline = Long.MAX_VALUE;
  private AddrAndPort testDest = new HostAndPort("localhost", 7777);

  private PersistentAsyncServer<Connection> createTestPersistentAsyncServer(int maxAttempts) {
    PersistentAsyncServer<Connection> pa = new PersistentAsyncServer<Connection>(mockAsyncServer,
        new NewConnectionTimeoutController() {
          @Override
          public int getMaxAttempts(AddrAndPort addrAndPort) {
            // backoff implementation starts from 0
            return maxAttempts - 1;
          }

          @Override
          public int getRelativeTimeoutMillisForAttempt(AddrAndPort addrAndPort, int attemptIndex) {
            return Integer.MAX_VALUE;
          }

          @Override
          public int getMaxRelativeTimeoutMillis(AddrAndPort addrAndPort) {
            return Integer.MAX_VALUE;
          }
        }, null, true, null, new UUIDBase());
    pa.setAddressStatusProvider(mockAddressStatusProvider);
    pa.setSuspectAddressListener(mockSuspectAddressListener);
    return pa;
  }

  private void throwExcecptionUntil(int successAtTryNum, Exception exception)
      throws IOException, AuthFailedException {
    doReturn(true).when(mockAddressStatusProvider).isHealthy(any());
    AtomicInteger currTryGlobal = new AtomicInteger(0);
    when(mockAsyncServer.newOutgoingConnection(any(InetSocketAddress.class), any())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        int currTry = currTryGlobal.incrementAndGet();
        if (currTry == successAtTryNum) {
          return mock(Connection.class);
        } else {
          throw exception;
        }
      }
    });
  }

  // Authentication failure case
  @Test
  public void failAuthAndSucceedBackoffPinger() throws IOException, AuthFailedException {
    // no early failure for authentication
    failAuthAndSucceedBackoff(true);
  }

  @Test
  public void failAuthAndSucceedBackoffNonPinger() throws IOException, AuthFailedException {
    failAuthAndSucceedBackoff(false);
  }

  private void failAuthAndSucceedBackoff(boolean isPinger) throws IOException, AuthFailedException {
    int maxTry = 3;
    PersistentAsyncServer pa = createTestPersistentAsyncServer(maxTry);
    doReturn(isPinger).when(mockAddressStatusProvider).isAddressStatusProviderThread();

    // succeed at last try
    throwExcecptionUntil(maxTry, new RetryableAuthFailedException("mock authen fail"));

    boolean failed = false;
    try {
      pa.getConnection(testDest, realtimeDeadline);
    } catch (ConnectException ex) {
      failed = true;
    }

    // shall remove the suspect if connection eventually succeeds
    verify(mockSuspectAddressListener, times(1)).removeSuspect(any(InetSocketAddress.class));
    assertFalse("Shall not fail within backoff", failed);
  }

  @Test
  public void failAuthAndFailBackoffPinger() throws IOException, AuthFailedException {
    // no early failure for authentication
    failAuthAndFailBackoff(true);
  }

  @Test
  public void failAuthAndFailBackoffNonPinger() throws IOException, AuthFailedException {
    failAuthAndFailBackoff(false);
  }

  private void failAuthAndFailBackoff(boolean isPinger) throws IOException, AuthFailedException {
    int maxTry = 3;
    PersistentAsyncServer pa = createTestPersistentAsyncServer(maxTry);
    doReturn(isPinger).when(mockAddressStatusProvider).isAddressStatusProviderThread();

    // succeed after last try (i.e. all tries will fail)
    String connInfo = "mock authen fail for fake conn";
    throwExcecptionUntil(maxTry + 1, new RetryableAuthFailedException(connInfo));

    boolean failed = false;
    boolean failedWithConnectionAbsorbException = false;
    try {
      pa.getConnection(testDest, realtimeDeadline);
    } catch (AuthFailedException ex) {
      failed = true;
      failedWithConnectionAbsorbException = ex.toString().contains(connInfo);
    }

    // shall add the suspect if connection eventually fails
    verify(mockSuspectAddressListener, times(1)).addSuspect(any(InetSocketAddress.class),
        eq(SuspectProblem.ConnectionEstablishmentFailed));
    assertTrue("Shall fail for all tries", failed && failedWithConnectionAbsorbException);
  }

  // Normal ConnectionException failure case
  @Test
  public void failConnExceptionAndSucceedBackoffPinger() throws IOException, AuthFailedException {
    failConnExceptionAndSucceedBackoff(true);
  }

  @Test
  public void failConnExceptionAndSucceedBackoffNonPinger() throws IOException, AuthFailedException {
    failConnExceptionAndSucceedBackoff(false);
  }

  @Test
  public void doNotAcceptConnWhenNotEnabled() throws IOException, AuthFailedException {
    ServerSocketChannel channel = mock(ServerSocketChannel.class);
    SocketChannel res = mock(SocketChannel.class);
    when(channel.accept()).thenReturn(res);
    doNothing().when(mockAsyncServer).closeChannel(any());
    doCallRealMethod().when(mockAsyncServer).accept(any());
    when(mockAsyncServer.isEnabled()).thenReturn(false);
    connectionStarted = false;
    when(mockAsyncServer.addConnection(res, true)).then(c -> {
      connectionStarted = true;
      return null;
    });

    mockAsyncServer.accept(channel);
    assertFalse(connectionStarted);
  }

  private void failConnExceptionAndSucceedBackoff(boolean isPinger) throws IOException, AuthFailedException {
    int maxTry = 3;
    PersistentAsyncServer pa = createTestPersistentAsyncServer(maxTry);
    doReturn(isPinger).when(mockAddressStatusProvider).isAddressStatusProviderThread();

    // succeed at last try
    throwExcecptionUntil(maxTry, new ConnectException("mock ConnectException fail"));

    boolean failed = false;
    boolean earlyFailedNoRetry = false;
    try {
      pa.getConnection(testDest, realtimeDeadline);
    } catch (ConnectException ex) {
      failed = true;
      earlyFailedNoRetry = ex.toString().contains("addressStatusProvider failed to connect: ");
    }

    if (isPinger) {
      assertTrue("Shall early fail without retry for pinger", failed && earlyFailedNoRetry);
    } else {
      verify(mockSuspectAddressListener, times(1)).removeSuspect(any(InetSocketAddress.class));
      assertFalse("Shall not fail within backoff", failed);
    }
  }

  @Test
  public void failConnExceptionAndFailBackoffPinger() throws IOException, AuthFailedException {
    // will early fail
    failConnExceptionAndFailBackoff(true);
  }

  @Test
  public void failConnExceptionAndFailBackoffNonPinger() throws IOException, AuthFailedException {
    failConnExceptionAndFailBackoff(false);
  }

  private void failConnExceptionAndFailBackoff(boolean isPinger) throws IOException, AuthFailedException {
    int maxTry = 3;
    PersistentAsyncServer pa = createTestPersistentAsyncServer(maxTry);
    doReturn(isPinger).when(mockAddressStatusProvider).isAddressStatusProviderThread();

    // succeed after last try (i.e. all tries will fail)
    String msg = "mock ConnectException fail";
    throwExcecptionUntil(maxTry + 1, new ConnectException(msg));

    boolean failed = false;
    boolean earlyFailedNoRetry = false;
    boolean failedWithConnectException = false;
    try {
      pa.getConnection(testDest, realtimeDeadline);
    } catch (ConnectException ex) {
      failed = true;
      earlyFailedNoRetry = ex.toString().contains("addressStatusProvider failed to connect: ");
      failedWithConnectException = ex.toString().contains(msg);
    }

    if (isPinger) {
      assertTrue("Shall early fail without retry for pinger", failed && earlyFailedNoRetry);
    } else {
      verify(mockAsyncServer, times(1)).informSuspectAddressListener(any(InetSocketAddress.class));
      assertTrue("Shall fail for all tries retry for non-pinger", failed && failedWithConnectException);
    }
  }

  // Normal SocketTimeout failure case
  @Test
  public void failSocketTimeoutAndSucceedBackoffPinger() throws IOException, AuthFailedException {
    // will early fail
    failSocketTimeoutAndSucceedBackoff(true);
  }

  @Test
  public void failSocketTimeoutAndSucceedBackoffNonPinger() throws IOException, AuthFailedException {
    failSocketTimeoutAndSucceedBackoff(false);
  }

  private void failSocketTimeoutAndSucceedBackoff(boolean isPinger) throws IOException, AuthFailedException {
    int maxTry = 3;
    PersistentAsyncServer pa = createTestPersistentAsyncServer(maxTry);
    doReturn(isPinger).when(mockAddressStatusProvider).isAddressStatusProviderThread();

    // succeed at last try
    throwExcecptionUntil(maxTry, new SocketTimeoutException("mock SocketTimeout fail"));

    boolean failed = false;
    boolean earlyFailedNoRetry = false;
    try {
      pa.getConnection(testDest, realtimeDeadline);
    } catch (ConnectException ex) {
      failed = true;
      earlyFailedNoRetry = ex.toString().contains("addressStatusProvider failed to connect: ");
    }

    if (isPinger) {
      assertTrue("Shall early fail without retry for pinger", failed && earlyFailedNoRetry);
    } else {
      verify(mockSuspectAddressListener, times(1)).removeSuspect(any(InetSocketAddress.class));
      assertFalse("Shall not fail within backoff", failed);
    }
  }

  @Test
  public void failSocketTimeoutAndFailBackoffPinger() throws IOException, AuthFailedException {
    // will early fail
    failSocketTimeoutAndFailBackoff(true);
  }

  @Test
  public void failSocketTimeoutAndFailBackoffNonPinger() throws IOException, AuthFailedException {
    failSocketTimeoutAndFailBackoff(false);
  }

  private void failSocketTimeoutAndFailBackoff(boolean isPinger) throws IOException, AuthFailedException {
    int maxTry = 3;
    PersistentAsyncServer pa = createTestPersistentAsyncServer(maxTry);
    doReturn(isPinger).when(mockAddressStatusProvider).isAddressStatusProviderThread();

    // succeed after last try (i.e. all tries will fail)
    String msg = "mock SocketTimeout fail";
    throwExcecptionUntil(maxTry + 1, new SocketTimeoutException(msg));

    boolean failed = false;
    boolean earlyFailedNoRetry = false;
    boolean failedWithSocketTimeoutException = false;
    try {
      pa.getConnection(testDest, realtimeDeadline);
    } catch (ConnectException ex) {
      failed = true;
      earlyFailedNoRetry = ex.toString().contains("addressStatusProvider failed to connect: ");
      failedWithSocketTimeoutException = ex.toString().contains(msg);
    }

    if (isPinger) {
      assertTrue("Shall early fail without retry for pinger", failed && earlyFailedNoRetry);
    } else {
      verify(mockSuspectAddressListener, times(1)).addSuspect(any(InetSocketAddress.class),
          eq(SuspectProblem.ConnectionEstablishmentFailed));
      assertTrue("Shall fail for all tries", failed && failedWithSocketTimeoutException);
    }
  }

  // Other case
  @Test
  public void failIOExceptionNoRetry() throws IOException, AuthFailedException {
    int maxTry = 3;
    PersistentAsyncServer pa = createTestPersistentAsyncServer(maxTry);
    doReturn(false).when(mockAddressStatusProvider).isAddressStatusProviderThread();

    throwExcecptionUntil(maxTry, new IOException("mock IOException"));
    boolean failedWithRuntimeException = false;
    try {
      pa.getConnection(testDest, realtimeDeadline);
    } catch (RuntimeException ex) {
      failedWithRuntimeException = ex.toString().contains("Unexpected IOException");

    }

    assertTrue("Shall fail for IOException", failedWithRuntimeException);
  }

  @Test
  public void failIfMarkedAsUnhealthy() throws ConnectException, AuthFailedException {
    int maxTry = 3;
    PersistentAsyncServer pa = createTestPersistentAsyncServer(maxTry);
    doReturn(false).when(mockAddressStatusProvider).isHealthy(any());

    boolean failedWithUnhealthyMark = false;
    try {
      pa.getConnection(testDest, realtimeDeadline);
    } catch (UnhealthyConnectionAttemptException ex) {
      failedWithUnhealthyMark = ex.toString().contains("Connection attempted to unhealthy address: ");
    }

    assertTrue("Shall fail if dest is unhealthy", failedWithUnhealthyMark);
  }

  class ConnectionManagerTestObjects {
    ConnectionManager<Connection> connectionManager = new ConnectionManager<>();

    IPAndPort local1 = new IPAndPort("127.0.0.1:1234");
    IPAndPort remote1 = new IPAndPort("10.206.53.98:1111");
    Connection connection1 = mock(MessageGroupConnection.class);

    IPAndPort local2 = new IPAndPort("127.0.0.2:2345");
    IPAndPort remote2 = new IPAndPort("10.206.53.99:2222");
    Connection connection2 = mock(MessageGroupConnection.class);

    IPAndPort local3 = new IPAndPort("127.0.0.2:3456");
    IPAndPort remote3 = new IPAndPort("10.206.53.97:3333");
    Connection connection3 = mock(MessageGroupConnection.class);

    ConnectionManagerTestObjects() {
      when(connection1.getRemoteIPAndPort()).thenReturn(remote1);
      when(connection1.getLocalIPAndPort()).thenReturn(local1);

      when(connection2.getRemoteIPAndPort()).thenReturn(remote2);
      when(connection2.getLocalIPAndPort()).thenReturn(local2);

      when(connection3.getRemoteIPAndPort()).thenReturn(remote3);
      when(connection3.getLocalIPAndPort()).thenReturn(local3);
    }
  }

  private void assertConnections(ConnectionManager manager, Connection... expected) {
    int expectedSize = expected.length;
    int actualSize = manager.getConnections().size();
    assertTrue("expectedSize:" + expectedSize + " actualSize:" + actualSize, expectedSize == actualSize);
    assertTrue("expected connections are different from actual",
        manager.getConnections().containsAll(Arrays.asList(expected)));
  }

  @Test
  public void connectionManagerTest() {
    ConnectionManagerTestObjects test = new ConnectionManagerTestObjects();
    test.connectionManager.addConnection(test.connection1);
    assertConnections(test.connectionManager, test.connection1);

    Connection connection1Copy = mock(MessageGroupConnection.class);
    when(connection1Copy.getRemoteIPAndPort()).thenReturn(test.remote1);
    when(connection1Copy.getLocalIPAndPort()).thenReturn(test.local1);
    test.connectionManager.addConnection(connection1Copy);
    assertConnections(test.connectionManager, connection1Copy);

    test.connectionManager.disconnectConnection(connection1Copy, "testing disconnect");
    assertConnections(test.connectionManager);

    test.connectionManager.addConnection(test.connection1);
    assertConnections(test.connectionManager, test.connection1);

    test.connectionManager.addConnection(test.connection2);
    assertConnections(test.connectionManager, test.connection1, test.connection2);

    test.connectionManager.addConnection(test.connection3);
    assertConnections(test.connectionManager, test.connection1, test.connection2, test.connection3);

    assertTrue(3 == test.connectionManager.disconnectAll("testing disconnect all"));
    assertConnections(test.connectionManager);
  }

  @Test
  public void connectionManagerLocalVMTest() {
    ConnectionManagerTestObjects test = new ConnectionManagerTestObjects();
    test.connectionManager.addConnection(test.connection1);
    assertConnections(test.connectionManager, test.connection1);

    //disconnect will remove connection
    assertTrue(1 == test.connectionManager.disconnectAll("testing disconnect all"));
    assertConnections(test.connectionManager);

    //adding the connection again
    test.connectionManager.addConnection(test.connection1);
    assertConnections(test.connectionManager, test.connection1);

    //so we have another ConnectionManager which holds the same connection but at the client side
    ConnectionManager<Connection> clientConnectionManager = new ConnectionManager<>();
    ConnectionManager.addManager(clientConnectionManager);
    Connection clientConnection = mock(MessageGroupConnection.class);
    when(clientConnection.getRemoteIPAndPort()).thenReturn(test.local1);
    when(clientConnection.getLocalIPAndPort()).thenReturn(test.remote1);
    clientConnectionManager.addConnection(clientConnection);
    assertConnections(clientConnectionManager, clientConnection);

    //as connection is in same VM now so disconnectAll will skip it
    assertTrue("expected 0 disconnects as connection is local to VM", test.connectionManager.disconnectAll("testing disconnect all") == 0);
    assertConnections(clientConnectionManager, clientConnection);
  }

  @Test
  public void disconnectAllExclusionResponderTest() {
    ConnectionManagerTestObjects test = new ConnectionManagerTestObjects();
    SelfExclusionResponder responder = new DisconnectAllExclusionResponder(test.connectionManager);
    test.connectionManager.addConnection(test.connection1);
    test.connectionManager.addConnection(test.connection2);
    test.connectionManager.addConnection(test.connection3);
    assertConnections(test.connectionManager, test.connection1, test.connection2, test.connection3);

    responder.onExclusion();
    assertConnections(test.connectionManager);
  }
}

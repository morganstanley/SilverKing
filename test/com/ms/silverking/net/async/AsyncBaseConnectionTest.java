package com.ms.silverking.net.async;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.net.AddrAndPort;
import com.ms.silverking.net.HostAndPort;
import com.ms.silverking.net.security.ConnectionAbsorbException;

public class AsyncBaseConnectionTest {
    private AddressStatusProvider mockAddressStatusProvider = mock(AddressStatusProvider.class);
    private SuspectAddressListener mockSuspectAddressListener = mock(SuspectAddressListener.class);
    private AsyncServer mockAsyncServer = mock(AsyncServer.class);
    private long realtimeDeadline = Long.MAX_VALUE;
    private AddrAndPort testDest = new HostAndPort("localhost", 7777);

    private PersistentAsyncServer<Connection> createTestPersistentAsyncServer(int maxAttempts) {
        PersistentAsyncServer<Connection> pa = new PersistentAsyncServer<Connection>(mockAsyncServer, new NewConnectionTimeoutController() {
            @Override
            public int getMaxAttempts(AddrAndPort addrAndPort) {
                // backoff implementation starts from 0
                return maxAttempts - 1;
            }

            @Override
            public long getRelativeTimeoutMillisForAttempt(AddrAndPort addrAndPort, int attemptIndex) {
                return Long.MAX_VALUE - SystemTimeUtil.skSystemTimeSource.absTimeMillis();
            }

            @Override
            public long getMaxRelativeTimeoutMillis(AddrAndPort addrAndPort) {
                return Long.MAX_VALUE - SystemTimeUtil.skSystemTimeSource.absTimeMillis();
            }
        }, null, true, null, new UUIDBase());
        pa.setAddressStatusProvider(mockAddressStatusProvider);
        pa.setSuspectAddressListener(mockSuspectAddressListener);
        return pa;
    }


    private void throwExcecptionUntil(int successAtTryNum, Exception exception) throws IOException, ConnectionAbsorbException {
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
    public void failAuthAndSucceedBackoffPinger() throws IOException, ConnectionAbsorbException{
        // no early failure for authentication
        failAuthAndSucceedBackoff(true);
    }

    @Test
    public void failAuthAndSucceedBackoffNonPinger() throws IOException, ConnectionAbsorbException{
        failAuthAndSucceedBackoff(false);
    }

    private void failAuthAndSucceedBackoff(boolean isPinger) throws IOException, ConnectionAbsorbException {
        int maxTry = 3;
        PersistentAsyncServer pa = createTestPersistentAsyncServer(maxTry);
        doReturn(isPinger).when(mockAddressStatusProvider).isAddressStatusProviderThread();

        // succeed at last try
        throwExcecptionUntil(maxTry, new ConnectionAbsorbException(null, "mock authen fail", null, true, null));

        boolean failed = false;
        try{
            pa.getConnection(testDest, realtimeDeadline);
        } catch (ConnectException ex) {
            failed = true;
        }

        // shall remove the suspect if connection eventually succeeds
        verify(mockSuspectAddressListener, times(1)).removeSuspect(any(InetSocketAddress.class));
        assertFalse("Shall not fail within backoff", failed);
    }

    @Test
    public void failAuthAndFailBackoffPinger() throws IOException, ConnectionAbsorbException{
        // no early failure for authentication
        failAuthAndFailBackoff(true);
    }

    @Test
    public void failAuthAndFailBackoffNonPinger() throws IOException, ConnectionAbsorbException{
        failAuthAndFailBackoff(false);
    }

    private void failAuthAndFailBackoff(boolean isPinger) throws IOException, ConnectionAbsorbException{
        int maxTry = 3;
        PersistentAsyncServer pa = createTestPersistentAsyncServer(maxTry);
        doReturn(isPinger).when(mockAddressStatusProvider).isAddressStatusProviderThread();

        // succeed after last try (i.e. all tries will fail)
        String connInfo = "mock authen fail for fake conn";
        throwExcecptionUntil(maxTry + 1, new ConnectionAbsorbException(null, connInfo, null, true, null));

        boolean failed = false;
        boolean failedWithConnectionAbsorbException = false;
        try{
            pa.getConnection(testDest, realtimeDeadline);
        } catch (ConnectException ex) {
            failed = true;
            failedWithConnectionAbsorbException = ex.toString().contains(connInfo);
        }

        // shall add the suspect if connection eventually fails
        verify(mockSuspectAddressListener, times(1)).addSuspect(any(InetSocketAddress.class), eq(SuspectProblem.ConnectionEstablishmentFailed));
        assertTrue("Shall fail for all tries", failed && failedWithConnectionAbsorbException);
    }


    // Normal ConnectionException failure case
    @Test
    public void failConnExceptionAndSucceedBackoffPinger() throws IOException, ConnectionAbsorbException {
        failConnExceptionAndSucceedBackoff(true);
    }

    @Test
    public void failConnExceptionAndSucceedBackoffNonPinger() throws IOException, ConnectionAbsorbException {
        failConnExceptionAndSucceedBackoff(false);
    }

    private void failConnExceptionAndSucceedBackoff(boolean isPinger) throws IOException, ConnectionAbsorbException {
        int maxTry = 3;
        PersistentAsyncServer pa = createTestPersistentAsyncServer(maxTry);
        doReturn(isPinger).when(mockAddressStatusProvider).isAddressStatusProviderThread();

        // succeed at last try
        throwExcecptionUntil(maxTry, new ConnectException("mock ConnectException fail"));

        boolean failed = false;
        boolean earlyFailedNoRetry = false;
        try{
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
    public void failConnExceptionAndFailBackoffPinger() throws IOException, ConnectionAbsorbException {
        // will early fail
        failConnExceptionAndFailBackoff(true);
    }

    @Test
    public void failConnExceptionAndFailBackoffNonPinger() throws IOException, ConnectionAbsorbException {
        failConnExceptionAndFailBackoff(false);
    }

    private void failConnExceptionAndFailBackoff(boolean isPinger) throws IOException, ConnectionAbsorbException {
        int maxTry = 3;
        PersistentAsyncServer pa = createTestPersistentAsyncServer(maxTry);
        doReturn(isPinger).when(mockAddressStatusProvider).isAddressStatusProviderThread();

        // succeed after last try (i.e. all tries will fail)
        String msg = "mock ConnectException fail";
        throwExcecptionUntil(maxTry + 1, new ConnectException(msg));

        boolean failed = false;
        boolean earlyFailedNoRetry = false;
        boolean failedWithConnectException = false;
        try{
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
    public void failSocketTimeoutAndSucceedBackoffPinger() throws IOException, ConnectionAbsorbException{
        // will early fail
        failSocketTimeoutAndSucceedBackoff(true);
    }

    @Test
    public void failSocketTimeoutAndSucceedBackoffNonPinger() throws IOException, ConnectionAbsorbException{
        failSocketTimeoutAndSucceedBackoff(false);
    }

    private void failSocketTimeoutAndSucceedBackoff(boolean isPinger) throws IOException, ConnectionAbsorbException{
        int maxTry = 3;
        PersistentAsyncServer pa = createTestPersistentAsyncServer(maxTry);
        doReturn(isPinger).when(mockAddressStatusProvider).isAddressStatusProviderThread();

        // succeed at last try
        throwExcecptionUntil(maxTry, new SocketTimeoutException("mock SocketTimeout fail"));

        boolean failed = false;
        boolean earlyFailedNoRetry = false;
        try{
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
    public void failSocketTimeoutAndFailBackoffPinger() throws IOException, ConnectionAbsorbException{
        // will early fail
        failSocketTimeoutAndFailBackoff(true);
    }

    @Test
    public void failSocketTimeoutAndFailBackoffNonPinger() throws IOException, ConnectionAbsorbException{
        failSocketTimeoutAndFailBackoff(false);
    }

    private void failSocketTimeoutAndFailBackoff(boolean isPinger) throws IOException, ConnectionAbsorbException{
        int maxTry = 3;
        PersistentAsyncServer pa = createTestPersistentAsyncServer(maxTry);
        doReturn(isPinger).when(mockAddressStatusProvider).isAddressStatusProviderThread();

        // succeed after last try (i.e. all tries will fail)
        String msg = "mock SocketTimeout fail";
        throwExcecptionUntil(maxTry + 1, new SocketTimeoutException(msg));

        boolean failed = false;
        boolean earlyFailedNoRetry = false;
        boolean failedWithSocketTimeoutException = false;
        try{
            pa.getConnection(testDest, realtimeDeadline);
        } catch (ConnectException ex) {
            failed = true;
            earlyFailedNoRetry = ex.toString().contains("addressStatusProvider failed to connect: ");
            failedWithSocketTimeoutException = ex.toString().contains(msg);
        }

        if (isPinger) {
            assertTrue("Shall early fail without retry for pinger", failed && earlyFailedNoRetry);
        } else {
            verify(mockSuspectAddressListener, times(1)).addSuspect(any(InetSocketAddress.class),eq(SuspectProblem.ConnectionEstablishmentFailed));
            assertTrue("Shall fail for all tries", failed && failedWithSocketTimeoutException);
        }
    }



    // Other case
    @Test
    public void failIOExceptionNoRetry() throws IOException, ConnectionAbsorbException {
        int maxTry = 3;
        PersistentAsyncServer pa = createTestPersistentAsyncServer(maxTry);
        doReturn(false).when(mockAddressStatusProvider).isAddressStatusProviderThread();

        throwExcecptionUntil(maxTry, new IOException("mock IOException"));
        boolean failedWithRuntimeException = false;
        try{
            pa.getConnection(testDest, realtimeDeadline);
        } catch (RuntimeException ex) {
            failedWithRuntimeException = ex.toString().contains("Unexpected IOException");

        }

        assertTrue("Shall fail for IOException", failedWithRuntimeException);
    }

    @Test
    public void failIfMarkedAsUnhealthy() throws ConnectException {
        int maxTry = 3;
        PersistentAsyncServer pa = createTestPersistentAsyncServer(maxTry);
        doReturn(false).when(mockAddressStatusProvider).isHealthy(any());

        boolean failedWithUnhealthyMark = false;
        try{
            pa.getConnection(testDest, realtimeDeadline);
        } catch (UnhealthyConnectionAttemptException ex) {
            failedWithUnhealthyMark = ex.toString().contains("Connection attempted to unhealthy address: ");
        }

        assertTrue("Shall fail if dest is unhealthy", failedWithUnhealthyMark);
    }
}

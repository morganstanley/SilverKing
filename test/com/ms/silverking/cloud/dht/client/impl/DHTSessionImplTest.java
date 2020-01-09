package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.time.AbsMillisTimeSource;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class DHTSessionImplTest {

    @Test
    public void testCheckForTimeouts() {
        long timestamp = 1546300800000L;
        int namespaceCount = 2;

        DHTSessionImpl session = mock(DHTSessionImpl.class);
        doCallRealMethod().when(session).checkForTimeouts();
        when(session.getAbsMillisTimeSource()).thenReturn(mock(AbsMillisTimeSource.class));
        when(session.getAbsMillisTimeSource().absTimeMillis()).thenReturn(timestamp);
        List<ClientNamespace> clientNamespaceList = new ArrayList<ClientNamespace>();
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
}

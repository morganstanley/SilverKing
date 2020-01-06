package com.ms.silverking.cloud.dht.client.impl;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import com.ms.silverking.cloud.dht.client.FailureCause;
import com.ms.silverking.cloud.dht.client.KeyedOperationException;
import com.ms.silverking.cloud.dht.client.OperationException;
import com.ms.silverking.cloud.dht.client.OperationState;
import com.ms.silverking.cloud.dht.client.StoredValue;
import com.ms.silverking.thread.lwt.BaseWorker;
import com.ms.silverking.thread.lwt.LWTPool;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AsyncRetrievalOperationImplTest {

    AsyncRetrievalOperationImpl<String, String> retrievalOperationMock;

    @Before
    public void init() {
        BaseWorker.class.getClassLoader().setClassAssertionStatus(BaseWorker.class.getName(), false);
        LWTPoolProvider.defaultNonConcurrentWorkPool = mock(LWTPool.class);

        retrievalOperationMock = (AsyncRetrievalOperationImpl<String, String>) mock(AsyncRetrievalOperationImpl.class);
        when(retrievalOperationMock.getOperationStateMap()).thenReturn(new HashMap<String, OperationState>());
        when(retrievalOperationMock.getPartialResults()).thenReturn(new HashMap<String, StoredValue>());
    }

    @After
    public void finalize() {
        BaseWorker.class.getClassLoader().setClassAssertionStatus(BaseWorker.class.getName(), true);
        LWTPoolProvider.defaultNonConcurrentWorkPool = null;
    }

    @Test
    public void testThrowFailedException() {

        Map<String, FailureCause> failureCauses = new HashMap<String, FailureCause>();
        failureCauses.put("Obj1", FailureCause.ERROR);
        failureCauses.put("Obj2", FailureCause.TIMEOUT);

        try {
            when(retrievalOperationMock.getFailureCauses()).thenReturn(failureCauses);
            doCallRealMethod().when(retrievalOperationMock).throwFailedException();

            retrievalOperationMock.throwFailedException();
        } catch (OperationException e) {
            String errorMessage = e.getMessage();
            for (Map.Entry<String, FailureCause> entry : failureCauses.entrySet()) {
                assert (errorMessage.contains(
                        String.format("%s%s%s", entry.getKey(), KeyedOperationException.getKeyValueDelimiter(), entry.getValue().name())));
            }
        }
    }

    @Test
    public void testThrowFailedExceptionLimitsKeys() {

        Map<String, FailureCause> failureCauses = new HashMap<String, FailureCause>();
        int keysLimit = KeyedOperationException.getKeysLimit();
        int failureNum = 20;
        while (failureNum > 0) {
            failureCauses.put("Obj" + failureNum, FailureCause.ERROR);
            failureNum--;
        }

        try {
            when(retrievalOperationMock.getFailureCauses()).thenReturn(failureCauses);
            doCallRealMethod().when(retrievalOperationMock).throwFailedException();

            retrievalOperationMock.throwFailedException();
        } catch (OperationException e) {
            String errorMessage = e.getMessage();
            assert (errorMessage.split(KeyedOperationException.getFailuresDelimiter()).length == keysLimit);
        }
    }
}

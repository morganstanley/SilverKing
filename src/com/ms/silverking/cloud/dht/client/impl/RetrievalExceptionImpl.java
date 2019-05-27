package com.ms.silverking.cloud.dht.client.impl;

import java.util.Map;

import com.ms.silverking.cloud.dht.client.FailureCause;
import com.ms.silverking.cloud.dht.client.OperationState;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.StoredValue;

public class RetrievalExceptionImpl extends RetrievalException {
    public RetrievalExceptionImpl(Map<Object, OperationState> operationState, Map<Object, FailureCause> failureCause,
            Map<Object, StoredValue> partialResults) {
        super(operationState, failureCause, partialResults);
    }

    public RetrievalExceptionImpl(String message, Map<Object, OperationState> operationState,
            Map<Object, FailureCause> failureCause, Map<Object, StoredValue> partialResults) {
        super(message, operationState, failureCause, partialResults);
    }

    public RetrievalExceptionImpl(String message, Throwable cause, Map<Object, OperationState> operationState,
            Map<Object, FailureCause> failureCause, Map<Object, StoredValue> partialResults) {
        super(message, cause, operationState, failureCause, partialResults);
    }

    public RetrievalExceptionImpl(Throwable cause, Map<Object, OperationState> operationState,
            Map<Object, FailureCause> failureCause, Map<Object, StoredValue> partialResults) {
        super(cause, operationState, failureCause, partialResults);
    }
}

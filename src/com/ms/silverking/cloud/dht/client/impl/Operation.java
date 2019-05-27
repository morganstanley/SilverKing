package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.OperationOptions;
import com.ms.silverking.cloud.dht.client.OpTimeoutController;

/**
 * Base operation class. An Operation is a static representation of the action
 * specified by the user and does not contain any dynamic state such as completion.
 */
abstract class Operation {
	private final OperationUUID	opUUID;
	private final ClientOpType	opType;
	protected final OperationOptions options;
	
	Operation(ClientOpType opType, OperationOptions options) {
        opUUID = new OperationUUID();
		this.opType = opType;
		this.options = options;
	}
	
	final ClientOpType getOpType() {
		return opType;
	}
	
	OperationUUID getUUID() {
		return opUUID;
	}
		
    protected String oidString() {
        return super.toString();
    }
    
    abstract OpTimeoutController getTimeoutController();
    
	@Override
	public String toString() {
		StringBuilder	sb;
		
		sb = new StringBuilder();
		sb.append(opType);
		sb.append(options);
		return sb.toString();
	}
}

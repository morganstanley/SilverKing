package com.ms.silverking.cloud.dht;

import java.util.Set;

import com.google.common.base.Preconditions;
import com.ms.silverking.cloud.dht.client.OpTimeoutController;
import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;

public class OperationOptions {
    private final OpTimeoutController   opTimeoutController;
    private final Set<SecondaryTarget>	secondaryTargets;
    
    static {
        ObjectDefParser2.addParserWithExclusions(OperationOptions.class, null, 
                                                 FieldsRequirement.ALLOW_INCOMPLETE, null);
    }
    
    public OperationOptions(OpTimeoutController opTimeoutController, Set<SecondaryTarget> secondaryTargets) {
        Preconditions.checkNotNull(opTimeoutController);
        this.opTimeoutController = opTimeoutController;
        this.secondaryTargets = secondaryTargets;
    }
    
    public OpTimeoutController getOpTimeoutController() {
        return opTimeoutController;
    }
    
    public Set<SecondaryTarget>	getSecondaryTargets() {
    	return secondaryTargets;
    }
    
    @Override
    public int hashCode() {
    	int	hashCode;
    	
    	hashCode = opTimeoutController.hashCode();
    	if (secondaryTargets != null) {
    		hashCode = hashCode ^ secondaryTargets.hashCode();
    	}   	
    	return hashCode;
    }
    
    @Override
    public boolean equals(Object other) {
    	OperationOptions	oOptions;
    	
    	oOptions = (OperationOptions)other;
    	if (!opTimeoutController.equals(oOptions.opTimeoutController)) {
    		return false;
    	} else {
            if (this.secondaryTargets != null) {
                if (oOptions.secondaryTargets != null) {
                    if (!this.secondaryTargets.equals(oOptions.secondaryTargets)) {
                        return false;
                    } else {
                    	return true;
                    }
                } else {
                    return false;
                }
            } else {
                if (oOptions.secondaryTargets != null) {
                    return false;
                } else {
                	return true;
                }
            }
    	}
    }
}

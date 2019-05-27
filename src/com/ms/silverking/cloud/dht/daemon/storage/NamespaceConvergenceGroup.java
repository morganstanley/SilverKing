package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import com.ms.silverking.cloud.common.OwnerQueryMode;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ConvergenceController2;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.InvalidTransitionException;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.RingState;
import com.ms.silverking.log.Log;

/**
 * Allows tracking the progress of a set of ConvergenceControllers as a group. 
 */
public class NamespaceConvergenceGroup {
    private final NCGListener       ncgListener;
    private final OwnerQueryMode    ownerQueryMode;
    private final Set<ConvergenceController2>  incompleteConvergenceControllers;
    private boolean frozen;
    
    NamespaceConvergenceGroup(NCGListener ncgListener, OwnerQueryMode ownerQueryMode) {
        this.ncgListener = ncgListener;
        this.ownerQueryMode = ownerQueryMode;
        this.incompleteConvergenceControllers = new ConcurrentSkipListSet<>();
    }
    
    public void addConvergenceController(ConvergenceController2 convergenceController) {
        if (frozen) {
            throw new RuntimeException("frozen");
        }
        incompleteConvergenceControllers.add(convergenceController);
    }
    
    void freeze() {
        if (frozen) {
            throw new RuntimeException("Multiple calls to freeze()");
        } else {
            frozen = true;
        }
    }
    
    public void setComplete(ConvergenceController2 convergenceController) {
        boolean complete;
        
        if (!frozen) {
            throw new RuntimeException("!frozen");
        }
        incompleteConvergenceControllers.remove(convergenceController);
        complete = incompleteConvergenceControllers.size() == 0;
        if (complete) {
        	try {
        		ncgListener.convergenceComplete(this);
        	} catch (InvalidTransitionException ite) {
        		if (ite.getExistingRingState() == RingState.ABANDONED) {
        			Log.warning("Ignoring completion of ncgListener as RingState == ABANDONED");
        		} else {
        			throw ite;
        		}
        	}
        }
    }

    public OwnerQueryMode getOwnerQueryMode() {
        return ownerQueryMode;
    }

    public void startConvergence() {
        freeze();
        
        ConvergenceController2	prevConvergenceController;

        /*
         * Create a chain of convergence controllers so that namespace convergence is worked
         * on serially. This helps to improve locality and hence efficiency. This also
         * helps to control the amount of RAM consumed at any one instant.
         */
        prevConvergenceController = null;
        for (ConvergenceController2 convergenceController : incompleteConvergenceControllers) {
        	if (prevConvergenceController != null) {
        		convergenceController.setChainNext(prevConvergenceController);
        	}
        	prevConvergenceController = convergenceController;
        }
        prevConvergenceController.startConvergence();
    }
}

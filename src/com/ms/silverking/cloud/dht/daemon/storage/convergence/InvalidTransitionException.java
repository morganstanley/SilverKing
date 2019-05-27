package com.ms.silverking.cloud.dht.daemon.storage.convergence;

public class InvalidTransitionException extends RuntimeException {
	private final RingState	existingRingState;
	private final RingState newRingState;
	
	public InvalidTransitionException(RingState existingRingState, RingState newRingState) {
		super(String.format("Invalid transition: %s -> %s", existingRingState, newRingState));
		this.existingRingState = existingRingState;
		this.newRingState = newRingState;
	}

	public RingState getExistingRingState() {
		return existingRingState;
	}
	
	public RingState getNewRingState() {
		return newRingState;
	}
}

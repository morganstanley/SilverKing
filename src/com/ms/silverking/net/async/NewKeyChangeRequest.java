package com.ms.silverking.net.async;

import java.nio.channels.SelectableChannel;

/**
 * Request sent to Selector
 */
public final class NewKeyChangeRequest extends KeyChangeRequest {
	private final ChannelRegistrationWorker	crWorker;
	
	public NewKeyChangeRequest(SelectableChannel channel, Type type, int newOps, 
								ChannelRegistrationWorker crWorker) {
		super(channel, type, newOps);
		this.crWorker = crWorker;
	}
	
	public NewKeyChangeRequest(SelectableChannel channel, Type type, ChannelRegistrationWorker crWorker) {
		this(channel, type, 0, crWorker);
	}
	
	public ChannelRegistrationWorker getChannelRegistrationWorker() {
		return crWorker;
	}
	
	@Override
	public int hashCode() {
		return super.hashCode() ^ crWorker.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		NewKeyChangeRequest	other;
		
		other = (NewKeyChangeRequest)obj;
		return this.crWorker == other.crWorker && super.equals(other); 
	}
	
	public String toString() {
		return super.toString() +":"+ crWorker;
	}
}

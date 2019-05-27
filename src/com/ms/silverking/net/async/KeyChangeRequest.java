package com.ms.silverking.net.async;

import java.nio.channels.SelectableChannel;

/**
 * Request sent to Selector
 */
public class KeyChangeRequest {
	private final SelectableChannel	channel;
	private final Type	type;
	private final int	newOps;
	
	public enum Type {ADD_OPS, REMOVE_OPS, ADD_AND_CHANGE_OPS, CANCEL_AND_CLOSE};
	
	public KeyChangeRequest(SelectableChannel channel, Type type, int newOps) {
		this.channel = channel;
		this.type = type;
		this.newOps = newOps;
	}
	
	public KeyChangeRequest(SelectableChannel channel, Type type) {
		this(channel, type, 0);
	}
	
	public final SelectableChannel getChannel() {
		return channel;
	}
	
	public final Type getType() {
		return type;
	}
	
	public final int getNewOps() {
		return newOps;
	}
	
	@Override
	public int hashCode() {
		return channel.hashCode() ^ type.hashCode()  ^ newOps;
	}
	
	@Override
	public boolean equals(Object obj) {
		KeyChangeRequest	other;
		
		other = (KeyChangeRequest)obj;
		return this.channel == other.channel && this.type == other.type && this.newOps == other.newOps;
	}
	
	public String toString() {
		return channel +":"+ type +":"+ newOps;
	}
}

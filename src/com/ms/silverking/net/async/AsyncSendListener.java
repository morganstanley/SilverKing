package com.ms.silverking.net.async;

import com.ms.silverking.id.UUIDBase;

/**
 * Callback to inform when a given data item has been sent or has failed. 
 */
public interface AsyncSendListener {
	public void sent(UUIDBase uuid);
	public void failed(UUIDBase uuid);
	public void timeout(UUIDBase uuid);
}

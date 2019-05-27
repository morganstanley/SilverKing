package com.ms.silverking.net.async;

import java.nio.channels.SelectionKey;

/**
 * Implemented by classes that need to perform work when the SelectorController
 * registers a channel with a selector.
 */
interface ChannelRegistrationWorker {
	public void channelRegistered(SelectionKey key);
}

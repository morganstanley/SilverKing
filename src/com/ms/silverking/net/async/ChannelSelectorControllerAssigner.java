package com.ms.silverking.net.async;

import java.nio.channels.SelectableChannel;
import java.util.List;

/**
 * Assigns Channels to SelectorControllers
 */
public interface ChannelSelectorControllerAssigner<T extends Connection> {
	public SelectorController<T> assignChannelToSelectorController(
				SelectableChannel channel,
				List<SelectorController<T>> selectorControllers);
}

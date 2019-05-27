package com.ms.silverking.net.async;

import java.nio.channels.SelectableChannel;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 */
public class RandomChannelSelectorControllerAssigner<T extends Connection> implements ChannelSelectorControllerAssigner<T> {
	public RandomChannelSelectorControllerAssigner() {
	}
	
	@Override
	/**
	 * Assigns, the given channel to a SelectorController chosen from the list.
	 * Does *not* add the channel to the SelectorController.
	 */
	public SelectorController<T> assignChannelToSelectorController(SelectableChannel channel,
			List<SelectorController<T>> selectorControllers) {
		int	index;
		
		index = ThreadLocalRandom.current().nextInt(selectorControllers.size());
		return selectorControllers.get(index);
	}
}

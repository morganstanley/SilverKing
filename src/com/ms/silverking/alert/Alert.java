package com.ms.silverking.alert;

public class Alert {
	private final String	context;
	private final int		level;
	private final String	key;
	private final String	message;
	
	public Alert(String context, int level, String key, String message) {
		this.context = context;
		this.level = level;
		this.key = key;
		this.message = message;
	}

	public String getContext() {
		return context;
	}

	public int getLevel() {
		return level;
	}

	public String getKey() {
		return key;
	}

	public String getMessage() {
		return message;
	}
	
	@Override
	public int hashCode() {
		return context.hashCode() ^ Integer.hashCode(level) ^ key.hashCode() ^ message.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		Alert	o;
		
		o = (Alert)obj;
		return context.equals(o.context)
				&& level == o.level
				&& key.equals(o.key)
				&& message.equals(o.key);
	}
	
	@Override
	public String toString() {
		return context +":"+ level +":"+ key +":"+ message;
	}
}

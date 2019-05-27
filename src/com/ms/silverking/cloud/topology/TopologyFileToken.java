package com.ms.silverking.cloud.topology;

class TopologyFileToken {
	private final Type		type;
	private final String	entry;
	
	enum Type {ENTRY, OPEN_BLOCK, CLOSE_BLOCK};
	
	private static final String	OPEN_BLOCK_DELIMITER = "{";
	private static final String	CLOSE_BLOCK_DELIMITER = "}";
	
	private static final TopologyFileToken	OPEN_BLOCK_TOKEN = new TopologyFileToken(Type.OPEN_BLOCK, null);
	private static final TopologyFileToken	CLOSE_BLOCK_TOKEN = new TopologyFileToken(Type.CLOSE_BLOCK, null);
	
	static TopologyFileToken[] parseLine(String def) {
		TopologyFileToken[]	tokens;
		String[]	defs;

		defs = def.split("\\s+");
		tokens = new TopologyFileToken[defs.length]; 
		for (int i = 0; i < tokens.length; i++) {
			tokens[i] = parse(defs[i]);
		}
		return tokens;
	}
	
	static TopologyFileToken parse(String def) {
		if (def.equals(OPEN_BLOCK_DELIMITER)) {
			return OPEN_BLOCK_TOKEN;
		} else if (def.equals(CLOSE_BLOCK_DELIMITER)) {
			return CLOSE_BLOCK_TOKEN;
		} else {
			return new TopologyFileToken(Type.ENTRY, def);
		}
	}
	
	TopologyFileToken(Type type, String entry) {
		this.type = type;
		this.entry = entry;
	}
	
	Type getType() {
		return type;
	}
	
	String getEntry() {
		return entry;
	}
	
	@Override
	public String toString() {
		return type +":"+ entry;
	}
}

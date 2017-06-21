package com.ms.silverking.cloud.dht.client.gen;

public class FunctionCall implements Expression {
	private final String	name;
	private final String[]	args;
	
	public FunctionCall(String name, String[] args) {
		this.name = name;
		this.args = args;
	}

	@Override
	public String evaluate(Context c) {
		switch (name) {
		case "Replace": return doReplace(c);
		case "SetOutputFile": return doSetOutputFile(c);
		default: throw new RuntimeException("Unknown function: "+ name);
		}
	}

	private String doReplace(Context c) {
		// (variable name, regex, replacement)
		return new Variable(args[0]).evaluate(c).replaceAll(args[1], args[2]);
	}

	private String doSetOutputFile(Context c) {
		// (variable name, suffix)
		c.setCurrentOutputFileName(new Variable(args[0]).evaluate(c) + args[1]);
		return null;
	}
}

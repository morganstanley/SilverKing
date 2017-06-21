package com.ms.silverking.cloud.dht.client.gen;

public class Variable implements Expression {
	private final String	name;
	
	public Variable(String name) {
		this.name = name;
	}
	
	@Override
	public String evaluate(Context c) {
		switch(name) {
		case "Class": return c.getCurrentClass().getSimpleName();
		case "Package": return c.getCurrentPackage().getName();
		case "methodName": return c.getCurrentMethod().getName();
		case "parameterName": return c.getCurrentParameter().getName();
		default: throw new RuntimeException("Unknown variable: "+ name);
		}
	}
}

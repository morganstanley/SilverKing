package com.ms.silverking.cloud.dht.client.gen;

import java.io.PrintStream;

public class CPPGeneratorBase {
	private final PrintStream	out;
	
	private static final int	tabSpacing = 4;
	private static final String	tabReplacement;
	
	static {
		StringBuffer	sb;
		
		sb = new StringBuffer();
		for (int i = 0; i < tabSpacing; i++) {
			sb.append(' ');
		}
		tabReplacement = sb.toString();
	}
	
	public CPPGeneratorBase(PrintStream out) {
		this.out = out;
	}
	
	public CPPGeneratorBase() {
		this(System.out);
	}

	//////////////////////////////////////////
	
	protected void p(String f, Object... args) {
		out.printf(f.replaceAll("\\t", tabReplacement) +"\n", args);
	}

	protected void p() {
		out.printf("\n");
	}
	
	//////////////////////////////////////////
	
	protected String ifndef(Class c) {
		return c.getSimpleName().toUpperCase() +"_H";
	}	
	
	protected String getNamespaceCPPEquivalent(Class c) {
		return c.getPackage().getName().replace('.', '_');
	}
}

package com.ms.silverking.collection;

import java.io.Serializable;

public class TupleBase implements Serializable {
	private static final long serialVersionUID = 7624118038203845139L;

	private final int	size;
	
	static final String	defaultTupleParsePattern = ",";
	
	TupleBase(int size) {
		this.size = size;
	}
	
	public int getSize() {
		return size;
	}
}

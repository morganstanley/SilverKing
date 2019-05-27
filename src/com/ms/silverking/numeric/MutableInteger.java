package com.ms.silverking.numeric;

public class MutableInteger {
	private int	value;
	
	public MutableInteger(int value) {
		this.value = value;
	}
	
	public MutableInteger() {
		this(0);
	}
	
	public int getValue() {
		return value;
	}
	
	public void increment() {
		value++;
	}
	
	public void decrement() {
		value--;
	}
	
	@Override
	public int hashCode() {
		return Integer.hashCode(value);
	}
	
	@Override
	public boolean equals(Object obj) {
		MutableInteger	o;
		
		o = (MutableInteger)obj;
		return o.value == value;
	}
	
	@Override
	public String toString() {
		return Integer.toString(value);
	}
}

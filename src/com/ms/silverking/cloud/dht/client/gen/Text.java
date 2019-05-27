package com.ms.silverking.cloud.dht.client.gen;

public class Text implements ParseElement {
	private final String	s;
	
	public Text(String s) {
		this.s = s;
	}
	
	@Override
	public String toString() {
		return s;
	}
}

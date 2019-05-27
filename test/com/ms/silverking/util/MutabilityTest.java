package com.ms.silverking.util;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class MutabilityTest {

	private static final Mutability immutable = Mutability.Immutable;
	private static final Mutability mutable   = Mutability.Mutable;

	@Test(expected=RuntimeException.class)
	public void testEnsureMutable_Exception() {
		immutable.ensureMutable();
	}
	
	@Test
	public void testEnsureMutable() {
		mutable.ensureMutable();
		assertTrue(true);
	}
	
	@Test(expected=RuntimeException.class)
	public void testEnsureImmutable_Exception() {
		mutable.ensureImmutable();
	}
	
	@Test
	public void testEnsureImmutable() {
		immutable.ensureImmutable();
		assertTrue(true);
	}

}

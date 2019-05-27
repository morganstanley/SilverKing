package com.ms.silverking.numeric;

public class NumArgValidator {
	public static void ensureRegularDouble(double d) {
		if (Double.isInfinite(d) || Double.isNaN(d)) {
			throw new IllegalArgumentException("Not a regular double: "+ d);
		}
	}

	public static void ensurePositiveRegularDouble(double d) {
		if (d <= 0.0) {
			throw new IllegalArgumentException("Not a positive double: "+ d);
		}
		ensureRegularDouble(d);
	}
}

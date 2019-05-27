package com.ms.silverking.test;

import java.math.BigDecimal;
import java.math.MathContext;

public class CollisionCalc {
	private static final MathContext	mc = MathContext.DECIMAL128;
	private static final BigDecimal		two = new BigDecimal(2);
	private static final BigDecimal		e = new BigDecimal(Math.E);
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("args: <bits> <numkeys>");
		} else {
			int		bits;
			double	numKeys;
			double	distinctKeys;
			double	pNoCollision;
			double	pCollision;
						
			bits = Integer.parseInt(args[0]);
			numKeys = Double.parseDouble(args[1]);
			
			distinctKeys = two.pow(bits, mc).doubleValue();
			pNoCollision = Math.exp(-numKeys * (numKeys - 1) / (2.0 * distinctKeys));
			pCollision = 1 - pNoCollision;
			System.out.printf("pCollision: %e\n", pCollision);
		}
	}
}

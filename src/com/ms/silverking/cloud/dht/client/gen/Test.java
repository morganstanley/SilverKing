package com.ms.silverking.cloud.dht.client.gen;


public class Test {

	public static void main(String[] args) {
		try {
			/*
			for (Method m : SynchronousReadableNamespacePerspective.class.getDeclaredMethods()) {
				System.out.println(m);
				for (Type t : m.getGenericParameterTypes()) {
					System.out.printf("\t%s\n", t);
				}
				System.out.printf("\t%s\n", m.getGenericReturnType());
			}
			*/
			/*
			for (Method m : ValueCreator.class.getMethods()) {
				System.out.println(m);
			}
			*/
			/*
			Method	m;
			
			m = ValueCreator.class.getMethod("toString", null);
			System.out.printf("%s\n", m);
			*/
			//System.out.println(new DHTClient().getValueCreator().toString());
			System.out.printf("%s\n", JNIUtil.getJNISignature(Object.class.getMethod("toString", null)));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

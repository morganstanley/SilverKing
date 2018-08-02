package com.ms.silverking.cloud.dht.management;

import static com.ms.silverking.testing.AssertFunction.test_SetterExceptions;
import static org.junit.Assert.*;

import org.junit.Test;

import com.ms.silverking.testing.Util.ExceptionChecker;

public class SKCloudAdminTest {

	// excludeMaster, by default master is included
	
	@Test
	public void testConstructor() {
		SKCloudAdmin cloudAdmin = new SKCloudAdmin(1);
	}

	@Test
	public void testConstructor_Exceptions() {
		Object[][] testCases = {
				{"numInstances = 0",                      new ExceptionChecker() { @Override public void check() { new SKCloudAdmin(0);                              } },         IllegalArgumentException.class},
//				{"amiId = null",                      new ExceptionChecker() { @Override public void check() { new SKCloudAdmin(null,    null);                              } },         NullPointerException.class},
//			{"instanceType = null",               new ExceptionChecker() { @Override public void check() { new SKCloudAdmin("valid", null);                      } },         NullPointerException.class},
		};
		
		test_SetterExceptions(testCases);
	}
}

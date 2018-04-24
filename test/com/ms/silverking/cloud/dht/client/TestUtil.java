package com.ms.silverking.cloud.dht.client;

import static com.ms.silverking.testing.Util.getTestMessage;
import static org.junit.Assert.assertEquals;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.time.AbsMillisTimeSource;
import com.ms.silverking.time.AbsNanosTimeSource;
import com.ms.silverking.time.ConstantAbsMillisTimeSource;
import com.ms.silverking.time.RelNanosTimeSource;
import com.ms.silverking.time.SafeAbsNanosTimeSource;
import com.ms.silverking.time.TimerDrivenTimeSource;

@OmitGeneration
public class TestUtil {

	public static int getMaxAttempts(OpTimeoutController controller) {
		return controller.getMaxAttempts(null);
	}
	
	public static int getRelativeTimeout_Null(OpTimeoutController controller) {
		return controller.getRelativeTimeoutMillisForAttempt(null, -1);
	}

//	public static int getRelativeTimeout_Snapshot(OpSizeBasedTimeoutController controller) {
//		return controller.getRelativeTimeoutMillisForAttempt(asyncSnapshot, -1);
//	}
//	
//	public static int getRelativeTimeout_Retrieval(OpSizeBasedTimeoutController controller) {
//		return controller.getRelativeTimeoutMillisForAttempt(asyncRetrieval, -1);
//	}
	
	public static int getMaxRelativeTimeout_Null(OpTimeoutController controller) {
		return controller.getMaxRelativeTimeoutMillis(null);
	}
	
//	public static int getMaxRelativeTimeout_Snapshot(OpSizeBasedTimeoutController controller) {
//		return controller.getMaxRelativeTimeoutMillis(asyncSnapshot);
//	}
//	
//	public static int getMaxRelativeTimeout_Retrieval(OpSizeBasedTimeoutController controller) {
//		return controller.getMaxRelativeTimeoutMillis(asyncRetrieval);
//	}


	public static void test_GetVersion(Object[][] testCases) {
		for (Object[] testCase : testCases) {
			long expected      =            (long)testCase[0];
			VersionProvider vp = (VersionProvider)testCase[1];
			
			assertEquals( getTestMessage("getVersion", expected, vp), expected, vp.getVersion());
		}
	}

	public static final AbsMillisTimeSource amts = new ConstantAbsMillisTimeSource(0);
	public static final AbsNanosTimeSource ants  = new SafeAbsNanosTimeSource(0);
	public static final long v                   = 0;
	public static final RelNanosTimeSource rnts  = new TimerDrivenTimeSource(1);
	
	public static final AbsMillisVersionProvider absMillisProvider = new AbsMillisVersionProvider(amts);
	public static final AbsNanosVersionProvider  absNanosProvider  = new AbsNanosVersionProvider(ants);
	public static final ConstantVersionProvider  constantProvider  = new ConstantVersionProvider(v);
	public static final RelNanosVersionProvider  relNanosProvider  = new RelNanosVersionProvider(rnts);
}

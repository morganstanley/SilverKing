package com.ms.silverking.cloud.dht.client;

import static com.ms.silverking.cloud.dht.client.OpSizeBasedTimeoutController.defaultExclusionChangeRetryIntervalMS;
import static com.ms.silverking.cloud.dht.client.OpSizeBasedTimeoutController.defaultMaxAttempts;
import static com.ms.silverking.cloud.dht.client.OpSizeBasedTimeoutController.defaultNonKeyedOpMaxRelTimeout_ms;
import static com.ms.silverking.cloud.dht.client.TestUtil.getMaxAttempts_Null;
import static com.ms.silverking.cloud.dht.client.TestUtil.getMaxRelativeTimeoutMillis_Null;
import static com.ms.silverking.cloud.dht.client.TestUtil.getRelativeExclusionChangeRetryMillisForAttempt_Null;
import static com.ms.silverking.cloud.dht.client.TestUtil.getRelativeTimeoutMillisForAttempt_Null;
import static com.ms.silverking.testing.AssertFunction.checkHashCodeEquals;
import static com.ms.silverking.testing.AssertFunction.checkHashCodeNotEquals;
import static com.ms.silverking.testing.AssertFunction.test_FirstEqualsSecond_FirstNotEqualsThird;
import static com.ms.silverking.testing.AssertFunction.test_Getters;
import static com.ms.silverking.testing.AssertFunction.test_NotEquals;
import static com.ms.silverking.testing.AssertFunction.test_SetterExceptions;
import static com.ms.silverking.testing.AssertFunction.test_Setters;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ms.silverking.testing.Util.ExceptionChecker;

public class OpSizeBasedTimeoutControllerTest {

	private static final int maCopy = 4;
	private static final int maDiff = 3;
	
	private static final int ctmCopy = 300_000;
	private static final int ctmDiff = 299_999;
	
	private static final int itmCopy = 305;
	private static final int itmDiff = 306;
	
	private static final int mrtomCopy = 1_500_000;
	private static final int mrtomDiff = 1_500_001;
	
	private static final int ecrimCopy = 5_000;
	private static final int ecrimDiff = 4_999;
	
	public  static final OpSizeBasedTimeoutController defaultController     =     OpSizeBasedTimeoutController.template;
	private static final OpSizeBasedTimeoutController defaultControllerCopy = new OpSizeBasedTimeoutController(maCopy, ctmCopy, itmCopy, mrtomCopy, ecrimCopy);
	private static final OpSizeBasedTimeoutController defaultControllerDiff = new OpSizeBasedTimeoutController(maDiff, ctmDiff, itmDiff, mrtomDiff, ecrimDiff);
	
//	private static final NamespacePerspectiveOptions<byte[], byte[]> nspOptions = Util.getCopy();
//	private static final AsyncRetrievalOperationImpl asyncRetrieval = new AsyncRetrievalOperationImpl(null, null, new NamespacePerspectiveOptionsImpl(nspOptions, SerializationRegistry.createEmptyRegistry()), 0, null);    
//	private static final AsyncSnapshotOperationImpl asyncSnapshot = new AsyncSnapshotOperationImpl();
	
	private OpSizeBasedTimeoutController setMaxAttempts(int ma) {
		return defaultController.maxAttempts(ma);
	}
	
	private OpSizeBasedTimeoutController setConstantTimeMillis(int ctm) {
		return defaultController.constantTimeMillis(ctm);
	}
	
	private OpSizeBasedTimeoutController setItemTimeMillis(int itm) {
		return defaultController.itemTimeMillis(itm);
	}
	
	private OpSizeBasedTimeoutController setNonKeyedOpMaxRelTimeoutMillis(int mrtom) {
		return defaultController.maxRelTimeoutMillis(mrtom);
	}
	
	// ALL THE "fixme"s for getRelativeTimeoutMillisForAttempt_(Snapshot|Retrieval) b/c Async Operation object is too hard to create.. 
	
	@Test
	public void testGetters() {
		Object[][] testCases = {
			{defaultMaxAttempts,                          getMaxAttempts_Null(defaultController)},
			{defaultNonKeyedOpMaxRelTimeout_ms,           getRelativeTimeoutMillisForAttempt_Null(defaultController)},
			{(long)defaultExclusionChangeRetryIntervalMS, getRelativeExclusionChangeRetryMillisForAttempt_Null(defaultController)},
//			{fixme, 							          getRelativeTimeoutMillisForAttempt_Snapshot(defaultController)},
//			{fixme,                                       getRelativeTimeoutMillisForAttempt_Retrieval(defaultController)},
			{defaultNonKeyedOpMaxRelTimeout_ms,           getMaxRelativeTimeoutMillis_Null(defaultController)},
//			{fixme, 							          getMaxRelativeTimeoutMillis_Snapshot(defaultController)},
//			{fixme,                                       getMaxRelativeTimeoutMillis_Retrieval(defaultController)},
		};
		
		test_Getters(testCases);
	}
	
	// this takes care of testing ctors as well b/c the class' setter functions implementation is calling the constructor - builder pattern
	@Test
	public void testSetters_Exceptions() {
		Object[][] testCases = {
			{"maxAttempts < min_maxAttempts", new ExceptionChecker() { @Override public void check() { setMaxAttempts(0); } }, RuntimeException.class},
		};

		test_SetterExceptions(testCases);
	}
	
	@Test
	public void testSetters() {
		OpSizeBasedTimeoutController       maController = setMaxAttempts(maDiff);
		OpSizeBasedTimeoutController      ctmController = setConstantTimeMillis(ctmDiff);
		OpSizeBasedTimeoutController      itmController = setItemTimeMillis(itmDiff);
		OpSizeBasedTimeoutController nkomrtomController = setNonKeyedOpMaxRelTimeoutMillis(mrtomDiff);

		Object[][] testCases = {
			{maDiff,     getMaxAttempts_Null(maController)},
			{mrtomCopy,  getRelativeTimeoutMillisForAttempt_Null(ctmController)},
//			{fixme,      getRelativeTimeoutMillisForAttempt_Snapshot(ctmController},
//			{fixme,      getRelativeTimeoutMillisForAttempt_Retrieval(ctmController},
			{mrtomCopy,  getMaxRelativeTimeoutMillis_Null(ctmController)},
//			{fixme,      getMaxRelativeTimeoutMillis_Snapshot(ctmController},
//			{fixme,      getMaxRelativeTimeoutMillis_Retrieval(ctmController},
			{mrtomCopy,  getRelativeTimeoutMillisForAttempt_Null(itmController)},
//			{fixme,      getRelativeTimeoutMillisForAttempt_Snapshot(itmController},
//			{fixme,      getRelativeTimeoutMillisForAttempt_Retrieval(itmController},
			{mrtomCopy,  getMaxRelativeTimeoutMillis_Null(itmController)},
//			{fixme,      getMaxRelativeTimeoutMillis_Snapshot(itmController},
//			{fixme,      getMaxRelativeTimeoutMillis_Retrieval(itmController},
			{mrtomDiff,  getRelativeTimeoutMillisForAttempt_Null(nkomrtomController)},
//			{nkomrtDiff, getRelativeTimeoutMillisForAttempt_Snapshot(nkomrtController},
//			{fixme,      getRelativeTimeoutMillisForAttempt_Retrieval(nkomrtController},
			{mrtomDiff,  getMaxRelativeTimeoutMillis_Null(nkomrtomController)},
//			{nkomrtDiff, getMaxRelativeTimeoutMillis_Snapshot(nkomrtController},
//			{fixme,      getMaxRelativeTimeoutMillis_Retrieval(nkomrtController},
		};
		
		test_Setters(testCases);
	}
	
	@Test
	public void testComputeTimeout() {
		int[][] testCases = {
			{0,     ctmCopy},
			{1,     ctmCopy+itmCopy},
			{3_934, 1_499_870},
			{3_935, mrtomCopy},
		};
		
		for (int[] testCase : testCases) {
			int param    = testCase[0];
			int expected = testCase[1];
			
			assertEquals(expected, defaultController.computeTimeout(param));
		}
	}
	
	@Test
	public void testHashCode() {
		checkHashCodeEquals(   defaultController, defaultController);
		checkHashCodeEquals(   defaultController, defaultControllerCopy);
		checkHashCodeNotEquals(defaultController, defaultControllerDiff);
	}
	
	@Test
	public void testEqualsObject() {
		OpSizeBasedTimeoutController[][] testCases = {
			{defaultController,     defaultController,                           defaultControllerDiff},
			{defaultControllerCopy, defaultController,                           defaultControllerDiff},
			{defaultControllerDiff, defaultControllerDiff,                       defaultController},
			{defaultController,     setMaxAttempts(maCopy),                      setMaxAttempts(maDiff)},
			{defaultController,     setConstantTimeMillis(ctmCopy),              setConstantTimeMillis(ctmDiff)},
			{defaultController,     setItemTimeMillis(itmCopy),                  setItemTimeMillis(itmDiff)},
			{defaultController,     setNonKeyedOpMaxRelTimeoutMillis(mrtomCopy), setNonKeyedOpMaxRelTimeoutMillis(mrtomDiff)},
		};
		test_FirstEqualsSecond_FirstNotEqualsThird(testCases);
		
		test_NotEquals(new Object[][]{
			{defaultController,  SimpleTimeoutController.template},
			{defaultController, WaitForTimeoutController.template},
		});
	}

	@Test
	public void testToStringAndParse() {
		OpSizeBasedTimeoutController[] testCases = {
			defaultController,
			defaultControllerCopy,
			defaultControllerDiff,
		};
		
		for (OpSizeBasedTimeoutController testCase : testCases)
			checkStringAndParse(testCase);
	}
	
	private void checkStringAndParse(OpSizeBasedTimeoutController controller) {
		assertEquals(controller, OpSizeBasedTimeoutController.parse( controller.toString() ));
	}
}

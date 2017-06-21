package com.ms.silverking.cloud.dht.client;

import static com.ms.silverking.cloud.dht.client.TestUtil.*;
import static com.ms.silverking.testing.AssertFunction.*;
import static com.ms.silverking.testing.AssertFunction.*;
import static com.ms.silverking.testing.AssertFunction.test_Getters;
import static com.ms.silverking.testing.AssertFunction.test_SetterExceptions;
import static com.ms.silverking.testing.AssertFunction.test_Setters;
import static org.junit.Assert.assertEquals;

import static com.ms.silverking.cloud.dht.client.OpSizeBasedTimeoutController.*;

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
	
	private static final OpSizeBasedTimeoutController defaultController     =     OpSizeBasedTimeoutController.template;
	private static final OpSizeBasedTimeoutController defaultControllerCopy = new OpSizeBasedTimeoutController(maCopy, ctmCopy, itmCopy, mrtomCopy);
	private static final OpSizeBasedTimeoutController defaultControllerDiff = new OpSizeBasedTimeoutController(maDiff, ctmDiff, itmDiff, mrtomDiff);
	
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
	
	@Test
	public void testGetters() {
		Object[][] testCases = {
			{defaultMaxAttempts,                getMaxAttempts(defaultController)},
			{defaultNonKeyedOpMaxRelTimeout_ms, getRelativeTimeout_Null(defaultController)},
//			{fixme, getRelativeTimeout_Snapshot(defaultController)},
//			{fixme,                             getRelativeTimeout_Retrieval(defaultController)},
			{defaultNonKeyedOpMaxRelTimeout_ms, getMaxRelativeTimeout_Null(defaultController)},
//			{fixme, getMaxRelativeTimeout_Async(defaultController)},
//			{fixme,                             getMaxRelativeTimeout_Retrieval(defaultController)},
		};
		
		test_Getters(testCases);
	}
	
	// this takes care of testing ctors as well
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
			{maDiff,                            getMaxAttempts(maController)},
			{mrtomCopy, getRelativeTimeout_Null(ctmController)},
//			{fixme, getRelativeTimeout_Async(ctmController},
//			{fixme,                             getRelativeTimeout_Retrieval(ctmController},
			{mrtomCopy, getMaxRelativeTimeout_Null(ctmController)},
//			{fixme, getMaxRelativeTimeout_Async(ctmController},
//			{fixme,                             getMaxRelativeTimeout_Retrieval(ctmController},
			{mrtomCopy, getRelativeTimeout_Null(itmController)},
//			{fixme, getRelativeTimeout_Async(itmController},
//			{fixme,                             getRelativeTimeout_Retrieval(itmController},
			{mrtomCopy, getMaxRelativeTimeout_Null(itmController)},
//			{fixme, getMaxRelativeTimeout_Async(itmController},
//			{fixme,                             getMaxRelativeTimeout_Retrieval(itmController},
			{mrtomDiff,                      getRelativeTimeout_Null(nkomrtomController)},
//			{nkomrtDiff,                        getRelativeTimeout_Async(nkomrtController},
//			{fixme,                             getRelativeTimeout_Retrieval(nkomrtController},
			{mrtomDiff,                      getMaxRelativeTimeout_Null(nkomrtomController)},
//			{nkomrtDiff,                        getMaxRelativeTimeout_Async(nkomrtController},
//			{fixme,                             getMaxRelativeTimeout_Retrieval(nkomrtController},
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
		Object[][] testCases = {
			{defaultController,     defaultController,                           defaultControllerDiff},
			{defaultControllerDiff, defaultControllerDiff,                       defaultController},
			{defaultControllerCopy, defaultController,                           defaultControllerDiff},
			{defaultController,     setMaxAttempts(maCopy),                      setMaxAttempts(maDiff)},
			{defaultController,     setConstantTimeMillis(ctmCopy),              setConstantTimeMillis(ctmDiff)},
			{defaultController,     setItemTimeMillis(itmCopy),                  setItemTimeMillis(itmDiff)},
			{defaultController,     setNonKeyedOpMaxRelTimeoutMillis(mrtomCopy), setNonKeyedOpMaxRelTimeoutMillis(mrtomDiff)},
		};
		
		test_FirstEqualsSecond_SecondNotEqualsThird(testCases);
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

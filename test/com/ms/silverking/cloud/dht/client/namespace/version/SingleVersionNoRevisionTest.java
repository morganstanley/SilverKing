package com.ms.silverking.cloud.dht.client.namespace.version;

import static com.ms.silverking.cloud.dht.NamespaceVersionMode.SINGLE_VERSION;
import static com.ms.silverking.cloud.dht.RevisionMode.NO_REVISIONS;
import static com.ms.silverking.cloud.dht.client.namespace.version.TestUtil.*;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.testing.Util;

import com.ms.silverking.testing.annotations.SkLarge;

@SkLarge
public class SingleVersionNoRevisionTest {

	private static SynchronousNamespacePerspective<String, String> syncNsp;
	
	private static final String namespaceName = "SingleVersionNoRevisionTest";
	
	@BeforeClass
	public static void setUpBeforeClass() throws ClientException, IOException {
		DHTSession session = createSession();
        Namespace ns       = createNamespace(session, namespaceName, SINGLE_VERSION, NO_REVISIONS); 
        syncNsp            = ns.openSyncPerspective(String.class, String.class);
	}
	
	@Test
	public void test_Put() throws PutException, RetrievalException {
        _put(     k1, v1);
        _checkGet(k1, v1);
	}

	// currently, failing, expecting exception, but operation is succeeding
	@Test(expected = PutException.class)
	public void test_Put_WithVersion() throws PutException {
        _putVersion(k2, v1, 1);
	}

	// currently, failing, should be write once
	@Test(expected = PutException.class)
	public void test_PutKeyTwice_SameValue() throws PutException {
        _put(k3, v1);
        _put(k3, v1);
	}

	@Test(expected = PutException.class)
	public void test_PutKeyTwice_DiffValue() throws PutException {
        _put(k4, v1);
        _put(k4, v2);
	}

	@Test(expected = PutException.class)
	public void test_PutKeyTwice_SameValue_WithVersion() throws PutException {
        _put(       k5, v1);
        _putVersion(k5, v1, 1);
	}

	private void _put(String k, String v) throws PutException {
		put(syncNsp, k, v);
	}
	
	private void _putVersion(String k, String v, int version) throws PutException {
		put(syncNsp, k, v, version);
	}

	private void _checkGet(String k, String expectedValue) throws RetrievalException {
		checkGet(syncNsp, k, expectedValue);
	}
	
	public static void main(String[] args) {
		Util.runTests(SingleVersionNoRevisionTest.class);
	}
}

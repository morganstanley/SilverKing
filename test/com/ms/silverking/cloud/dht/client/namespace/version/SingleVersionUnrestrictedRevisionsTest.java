package com.ms.silverking.cloud.dht.client.namespace.version;

import static com.ms.silverking.cloud.dht.NamespaceVersionMode.SINGLE_VERSION;
import static com.ms.silverking.cloud.dht.RevisionMode.UNRESTRICTED_REVISIONS;
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
public class SingleVersionUnrestrictedRevisionsTest {

	private static SynchronousNamespacePerspective<String, String> syncNsp;

	private static final String namespaceName = SingleVersionUnrestrictedRevisionsTest.class.getSimpleName();

	@BeforeClass
	public static void setUpBeforeClass() throws ClientException, IOException {
		DHTSession session = createSession();
        Namespace ns       = createNamespace(session, namespaceName, SINGLE_VERSION, UNRESTRICTED_REVISIONS); 
        syncNsp            = ns.openSyncPerspective(String.class, String.class);
	}
	
	@Test
	public void test_Put() throws PutException, RetrievalException {
        _put(     k1, v1);
        _checkGet(k1, v1);
	}

	// currently, failing, expected exception but operation is succeeding (user shouldn't be allowed to pass any version when in SINGLE_VERSION mode, it's all taken care for him behind the scenes)
	@Test(expected = PutException.class)
	public void test_Put_WithVersion() throws PutException {
        _putVersion(k2, v1, version1);
	}

	@Test
	public void test_PutKeyTwice_SameValue() throws PutException, RetrievalException {
        _put(             k3, v1);
        _put(             k3, v1);
        _checkGet(        k3, v1);
        _checkGetLeast(   k3, v1);
        _checkGetGreatest(k3, v1);
	}

	// failing on second put
	@Test
	public void test_PutKeyTwice_DiffValue() throws PutException, RetrievalException {
        _put(             k4, v1);
        _put(             k4, v2);
        _checkGet(        k4, v2);
        _checkGetLeast(   k4, v1);
        _checkGetGreatest(k4, v2);
	}

	// failing, user shouldn't be allowed to pass any version when in SINGLE_VERSION mode, it's all taken care for him behind the scenes
	@Test(expected = PutException.class)
	public void test_PutKeyTwice_SameValue_WithVersion() throws PutException {
		_put(       k5, v1);
        _putVersion(k5, v1, version1);
	}

	@Test(expected = PutException.class)
	public void test_PutKeyTwice_DiffValue_WithVersion() throws PutException {
		_put(       k6, v1);
        _putVersion(k6, v2, version1);
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
	
	private void _checkGetLeast(String k, String expectedValue) throws RetrievalException {
        checkGetLeast(syncNsp, k, expectedValue);
	}
	
	private void _checkGetGreatest(String k, String expectedValue) throws RetrievalException {
        checkGetGreatest(syncNsp, k, expectedValue);
	}
	
	public static void main(String[] args) {
		Util.runTests(SingleVersionUnrestrictedRevisionsTest.class);
	}
}

package com.ms.silverking.cloud.dht.client.namespace.version;

import static com.ms.silverking.cloud.dht.client.namespace.version.TestUtil.*;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import static com.ms.silverking.cloud.dht.NamespaceVersionMode.*;

import static com.ms.silverking.cloud.dht.RevisionMode.*;

import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.testing.Util;

import com.ms.silverking.testing.annotations.SkLarge;

@SkLarge
public class ClientSpecifiedVersionUnrestrictedRevisionsTest {

	private static SynchronousNamespacePerspective<String, String> syncNsp;
	
	private static final String namespaceName = "ClientSpecifiedUnrestrictedRevisionsTest";

	private static final int version1 = 1;
	private static final int version2 = 2;
	private static final int version3 = 3;
	private static final int version5 = 5;
	
	@BeforeClass
	public static void setUpBeforeClass() throws ClientException, IOException {
		DHTSession session = createSession();
        Namespace ns       = createNamespace(session, namespaceName+"bennnn", CLIENT_SPECIFIED, UNRESTRICTED_REVISIONS); 
        syncNsp            = ns.openSyncPerspective(String.class, String.class);
	}

	@Test
	public void test_PutKeyTwice_SameValue() throws PutException, RetrievalException {
		_put(k1, v1);
		_put(k1, v1);
		checkGets(k1, v1, v1);
	}

	// failing
	@Test
	public void test_PutKeyTwice_DiffValue() throws PutException, RetrievalException {
		_put(k2, v1);
        _put(k2, v2);
        checkGets(k2, v1, v2);
	}
	
	private void checkGets(String k, String valueLeast, String valueGreatest) throws RetrievalException {
        _checkGet(        k, valueGreatest);
        _checkGetLeast(   k, valueLeast);
        _checkGetGreatest(k, valueGreatest);
	}

	// failing b/c of version
	@Test
	public void test_PutKeyTwice_SameValue_WithVersion() throws PutException, RetrievalException {
		_put(               k3, v1);
		_putVersion(        k3, v1, version1);
		checkGetsWithVersion(k3, v1, v1, version1);
	}

	@Test
	public void test_PutKeyTwice_DiffValue_WithVersion() throws PutException, RetrievalException {
		_put(                k4, v1);
		_putVersion(         k4, v2, version2);
		checkGetsWithVersion(k4, v2, v1, version2);
	}

	private void checkGetsWithVersion(String k, String valueLeast, String valueGreatest, int version) throws RetrievalException {
		checkGets(k, valueLeast, valueGreatest);
		_checkGetExactMatch(k, valueLeast, version);
		_checkGetLeastVersion(     k, version,  true);
		_checkGetGreatestVersion(  k, version, false);
		_checkGetExactMatchVersion(k, version);
	}
	
	@Test
	public void test_PutKeyTwice_DiffValue_WithVersion_Increasing_Linear() throws PutException, RetrievalException {
		_putVersion(        k5, v1, version1);
		_putVersion(        k5, v2, version2);
		checkGets_Twice_Linear(k5);
	}

	@Test
	public void test_PutKeyTwice_DiffValue_WithVersion_Decreasing_Linear() throws PutException, RetrievalException {
		_putVersion(k6, v2, version2);
		_putVersion(k6, v1, version1);
		checkGets_Twice_Linear(k6);
	}
	
	private void checkGets_Twice_Linear(String k) throws RetrievalException {
		checkGets(k, v1, v2);
        _checkGetExactMatch(k, v1, version1);
		_checkGetExactMatch(k, v2, version2);
		_checkGetLeastVersion(     k, version1, true);
		_checkGetGreatestVersion(  k, version2, true);
		_checkGetExactMatchVersion(k, version1);
		_checkGetExactMatchVersion(k, version2);
	}

	@Test
	public void test_PutKeyThrice_DiffValue_WithVersion_Increasing_Skipping() throws PutException, RetrievalException {
		_putVersion(k7, v1, version1);
		_putVersion(k7, v3, version3);
		_putVersion(k7, v5, version5);
		checkGets_Thrice_Skipping(k7);
	}

	@Test
	public void test_PutKeyThrice_DiffValue_WithVersion_Decreasing_Skipping() throws PutException, RetrievalException {
		_putVersion(k8, v5, version5);
		_putVersion(k8, v3, version3);
		_putVersion(k8, v1, version1);
		checkGets_Thrice_Skipping(k8);
	}
	
	private void checkGets_Thrice_Skipping(String k) throws RetrievalException {
		checkGets(k, v1, v5);
        _checkGetExactMatch(k, v1, version1);
        _checkGetExactMatch(k, v3, version3);
		_checkGetExactMatch(k, v5, version5);
		_checkGetLeastVersion(     k, version1, true);
		_checkGetGreatestVersion(  k, version5, true);
		_checkGetExactMatchVersion(k, version1);
		_checkGetExactMatchVersion(k, version3);
		_checkGetExactMatchVersion(k, version5);
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
	
	private void _checkGetExactMatch(String k, String expectedValue, int version) throws RetrievalException {
		checkGetExactMatch(syncNsp, k, expectedValue, version);
	}

	private void _checkGetLeastVersion(String k, int version, boolean shouldMatch) throws RetrievalException {
		checkGetLeastVersion(syncNsp, k, shouldMatch, version);
	}

	private void _checkGetGreatestVersion(String k, int version, boolean shouldMatch) throws RetrievalException {
		checkGetGreatestVersion(syncNsp, k, shouldMatch, version);
	}

	private void _checkGetExactMatchVersion(String k, int version) throws RetrievalException {
		checkGetExactMatchVersion(syncNsp, k, version);
	}
	
	public static void main(String[] args) {
		Util.runTests(ClientSpecifiedVersionUnrestrictedRevisionsTest.class);
	}
}

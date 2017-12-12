package com.ms.silverking.cloud.dht.client.namespace.version;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import com.ms.silverking.cloud.dht.GetOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.NamespaceCreationException;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.StoredValue;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.testing.Util;

public class TestUtil {

	static final String k1 = "k1";
	static final String v1 = "v1";
	
	static final String k2 = "k2";
	static final String v2 = "v2";
	
	static final String k3 = "k3";
	static final String v3 = "v3";
	
	static final String k4 = "k4";
	
	static final String k5 = "k5";
	static final String v5 = "v5";

	static final String k6 = "k6";
	static final String k7 = "k7";
	static final String k8 = "k8";
	static final String k9 = "k9";
	static final String k10 = "k10";

	static final int version1 = 1;
	static final int version2 = 2;
	static final int version3 = 3;
	static final int version5 = 5;
	
	public static DHTSession createSession() throws ClientException, IOException {
		return new DHTClient().openSession( Util.getTestGridConfig() );
	}
	
	public static Namespace createNamespace(DHTSession session, String namespaceName, NamespaceVersionMode versionMode, RevisionMode revisionMode) throws NamespaceCreationException {
		return session.createNamespace(namespaceName, session.getDefaultNamespaceOptions().versionMode(versionMode).revisionMode(revisionMode));
	}
	
	static void put(SynchronousNamespacePerspective<String,String> nsp, String key, String value) throws PutException {
		nsp.put(key, value);
	}

	static void put(SynchronousNamespacePerspective<String,String> nsp, String key, String value, int version) throws PutException {
		put(nsp, key, value, nsp.getOptions().getDefaultPutOptions().version(version));
	}
	
	static void put(SynchronousNamespacePerspective<String,String> nsp, String key, String value, PutOptions po) throws PutException {
		nsp.put(key, value, po);
	}
	
	static void checkGet(SynchronousNamespacePerspective<String,String> nsp, String key, String expectedValue) throws RetrievalException {
		assertEquals( Util.getTestMessage("checkGet", "key: " + key, "expectedVal: " + expectedValue), expectedValue, nsp.get(key));
	}
	
	static void checkGetLeast(SynchronousNamespacePerspective<String,String> nsp, String key, String expectedValue) throws RetrievalException {
		checkGetValue("Least", nsp, key, expectedValue, VersionConstraint.least);
	}
	
	static void checkGetGreatest(SynchronousNamespacePerspective<String,String> nsp, String key, String expectedValue) throws RetrievalException {
		checkGetValue("Greatest", nsp, key, expectedValue, VersionConstraint.greatest);
	}
	
	static void checkGetExactMatch(SynchronousNamespacePerspective<String,String> nsp, String key, String expectedValue, int version) throws RetrievalException {
		checkGetValue("ExactMatch", nsp, key, expectedValue, VersionConstraint.exactMatch(version));
	}
	
	private static void checkGetValue(String testName, SynchronousNamespacePerspective<String,String> nsp, String key, String expectedValue, VersionConstraint constraint) throws RetrievalException {
//		System.out.println("version is: " + nsp.get(key, nsp.getOptions().getDefaultGetOptions().versionConstraint(constraint)).getVersion());
		assertEquals( Util.getTestMessage("checkGet"+testName+"Value", "key: " + key, "expectingVal: " + expectedValue, constraint), expectedValue, nsp.get(key, nsp.getOptions().getDefaultGetOptions().versionConstraint(constraint)).getValue());
	}

	static void checkGetLeastVersion(SynchronousNamespacePerspective<String,String> nsp, String key, boolean shouldMatch, int version) throws RetrievalException {
		checkGetVersion("Least", nsp, key, shouldMatch, version, VersionConstraint.least);
	}

	static void checkGetGreatestVersion(SynchronousNamespacePerspective<String,String> nsp, String key, boolean shouldMatch, int version) throws RetrievalException {
		checkGetVersion("Greatest", nsp, key, shouldMatch, version, VersionConstraint.greatest);
	}

	static void checkGetExactMatchVersion(SynchronousNamespacePerspective<String,String> nsp, String key, int version) throws RetrievalException {
		checkGetVersion("ExactMatch", nsp, key, true, version, VersionConstraint.exactMatch(version));
	}
	
	static void checkGetVersion(String testName, SynchronousNamespacePerspective<String,String> nsp, String key, boolean shouldMatch, int version, VersionConstraint constraint) throws RetrievalException {
		assertEquals( Util.getTestMessage("checkGet"+testName+"Version", "key: " + key, version), shouldMatch, nsp.get(key, nsp.getOptions().getDefaultGetOptions().versionConstraint(constraint)).getVersion() == version);
	}

	public static void printKeyVals(SynchronousNamespacePerspective<String, String> syncNsp, String key, GetOptions getOptions) throws RetrievalException {
		System.out.println("----------------");
		print(syncNsp, key, getOptions, 0);
		print(syncNsp, key, getOptions, 1);
		print(syncNsp, key, getOptions, 2);
		print(syncNsp, key, getOptions, 3);
		print(syncNsp, key, getOptions, 4);
		print(syncNsp, key, getOptions, 5);
		print(syncNsp, key, getOptions, 6);
		print(syncNsp, key, getOptions, 7);
		print(syncNsp, key, getOptions, 8);
		print(syncNsp, key, getOptions, 9);
		print(syncNsp, key, getOptions, 10);
		System.out.println("get:          " + syncNsp.get(key));
		System.out.println("get least:    " + print(syncNsp, key, getOptions, VersionConstraint.least));
		System.out.println("get greatest: " + print(syncNsp, key, getOptions, VersionConstraint.greatest));
		System.out.println("----------------");
	}
	
	public static void print(SynchronousNamespacePerspective<String, String> syncNsp, String key, GetOptions getOptions, int num) throws RetrievalException {
		System.out.println(num + " = " + print(syncNsp, key, getOptions, VersionConstraint.exactMatch(num)));
	}
	
	public static String print(SynchronousNamespacePerspective<String, String> syncNsp, String key, GetOptions getOptions, VersionConstraint vc) throws RetrievalException {
		StoredValue<String> val =  syncNsp.get(key, getOptions.versionConstraint(vc));
		return (val == null) ? "null" : val.getValue();
	}
}

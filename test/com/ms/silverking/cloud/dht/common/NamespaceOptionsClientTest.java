package com.ms.silverking.cloud.dht.common;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import static com.ms.silverking.cloud.dht.NamespaceVersionMode.*;
import static com.ms.silverking.cloud.dht.RevisionMode.*;

import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.testing.Util;
import com.ms.silverking.cloud.dht.client.NamespaceCreationException;

import static com.ms.silverking.cloud.dht.client.namespace.version.TestUtil.*;

import com.ms.silverking.testing.annotations.SkLarge;

@SkLarge
public class NamespaceOptionsClientTest {

	private static DHTSession session;
	private static final String namespaceName = "NamespaceOptionsClientTest";

	@BeforeClass
	public static void setUpBeforeClass() throws ClientException, IOException {
        session = createSession();
		_createNamespace(SINGLE_VERSION, UNRESTRICTED_REVISIONS);
	}
	
	@Test(expected = NamespaceCreationException.class)
	public void test() throws ClientException, IOException {
		_createNamespace(CLIENT_SPECIFIED, UNRESTRICTED_REVISIONS);
	}
	
	private static void _createNamespace(NamespaceVersionMode versionMode, RevisionMode revisionMode) throws NamespaceCreationException {
		createNamespace(session, namespaceName, versionMode, revisionMode);
	}
	
	public static void main(String[] args) {
		Util.runTests(NamespaceOptionsClientTest.class);
	}
}

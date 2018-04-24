package com.ms.silverking.cloud.dht.client.namespace;

import static com.ms.silverking.cloud.dht.client.namespace.version.TestUtil.*;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ms.silverking.cloud.dht.GetOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.ConstantVersionProvider;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.VersionProvider;
import com.ms.silverking.testing.Util;



import com.ms.silverking.testing.annotations.SkLarge;

@SkLarge
public class PerspectiveTest {

	private static DHTSession session;
	private static final String namespaceName = "PerspectiveTest";

	@BeforeClass
	public static void setUpBeforeClass() throws ClientException, IOException {
        session = createSession();
	}
	
//	@Test
	public void test_NamespaceOptions_vs_NamespacePerspectiveOptions() throws ClientException, IOException {
        Namespace ns = session.createNamespace(namespaceName, session.getDefaultNamespaceOptions().versionMode(NamespaceVersionMode.CLIENT_SPECIFIED).revisionMode(RevisionMode.UNRESTRICTED_REVISIONS)); 
        SynchronousNamespacePerspective<String, String>  syncNSPString = ns.openSyncPerspective(String.class,  String.class);
        SynchronousNamespacePerspective<String, Integer> syncNSPInt    = ns.openSyncPerspective(String.class, Integer.class);

        PutOptions poString = syncNSPString.getOptions().getDefaultPutOptions();
        GetOptions goString = syncNSPString.getOptions().getDefaultGetOptions();
        PutOptions poInt    = syncNSPInt.getOptions().getDefaultPutOptions();
        GetOptions goInt    = syncNSPInt.getOptions().getDefaultGetOptions();
        
        syncNSPString.put("k", "v1", poString.version(1));
        syncNSPInt.put(   "k",    2,    poInt.version(2));
        
        System.out.println(syncNSPString);
        System.out.println( syncNSPString.get("k"));
        System.out.println( syncNSPString.get("k", goString.versionConstraint(VersionConstraint.exactMatch(1))).getValue());
        System.out.println("-");
        System.out.println( syncNSPInt.get("k"));
        System.out.println( syncNSPInt.get("k", goInt.versionConstraint(VersionConstraint.exactMatch(2))).getValue());
        System.out.println( syncNSPInt.get("k", goInt.versionConstraint(VersionConstraint.exactMatch(1))).getValue());
	}
	
	@Test
	public void testDefaultsPuts() throws ClientException, IOException {
        Namespace ns = session.createNamespace(namespaceName, session.getDefaultNamespaceOptions().versionMode(NamespaceVersionMode.CLIENT_SPECIFIED).revisionMode(RevisionMode.UNRESTRICTED_REVISIONS).defaultPutOptions(session.getDefaultPutOptions().version(5))); 
        SynchronousNamespacePerspective<String, String>  syncNsp = ns.openSyncPerspective(String.class, String.class);

        PutOptions putOptions = syncNsp.getOptions().getDefaultPutOptions();
        GetOptions getOptions = syncNsp.getOptions().getDefaultGetOptions();

        syncNsp.put("k", "v1");
        printKeyVals(syncNsp, "k", getOptions);

        syncNsp.put("k", "v2", putOptions.version(1));
        printKeyVals(syncNsp, "k", getOptions);

        syncNsp.put("k", "v3");
        printKeyVals(syncNsp, "k", getOptions);
        
        // this guy is not working as expected
        syncNsp.setDefaultVersion(4);
        syncNsp.put("k", "v4");
        printKeyVals(syncNsp, "k", getOptions);        

        // this guy is not working as expected
        syncNsp.setDefaultVersionProvider(new ConstantVersionProvider(6));
        syncNsp.put("k", "v5");	
        printKeyVals(syncNsp, "k", getOptions);        

        syncNsp.put("k", "v6", putOptions.version(8));
        printKeyVals(syncNsp, "k", getOptions);
	}
	
//	@Test
	public void testDefaultGets() {
		
	}
	
//	@Test
	public void test() {
		try {
            SynchronousNamespacePerspective<String, String> syncNSP = new DHTClient().openSession(Util.getTestGridConfig()).openSyncNamespacePerspective("_VersionTest", String.class, String.class);
            syncNSP.setDefaultRetrievalVersionConstraint(VersionConstraint.defaultConstraint);
//            syncNSP.setDefaultVersionProvider(syncNSP.getNamespace().getOptions().versionMode(NamespaceVersionMode.CLIENT_SPECIFIED).getVersionMode());
//            syncNSP.set
            printNsoAndNspo(syncNSP);
            
            PutOptions po = syncNSP.getOptions().getDefaultPutOptions();
            GetOptions go = syncNSP.getOptions().getDefaultGetOptions();
//    		syncNSP.setOptions( syncNSP.getOptions().defaultPutOptions( po ));
            
    		System.out.println("\n\n"+go);
    		
    		syncNSP.getNamespace().getOptions().versionMode(NamespaceVersionMode.CLIENT_SPECIFIED).revisionMode(RevisionMode.UNRESTRICTED_REVISIONS);

    		printNsoAndNspo(syncNSP);
            
            syncNSP.put("k", "v1", po.version(1));
//            syncNSP.put("k", "v2", po.version(2));
            System.out.println( syncNSP.get("k"));
//            System.out.println( syncNSP.get("k", go));
//            syncNSP.setOptions( syncNSP.getOptions().defaultPutOptions( new PutOptions(opTimeoutController, secondaryTargets, compression, checksumType, checksumCompressedValues, version, userData)));
//            
//            syncNSP.put(mapA);
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	private void printNsoAndNspo(SynchronousNamespacePerspective<String, String> syncNSP) {
		System.out.println("\n\n+++NSO:\n"+syncNSP.getNamespace().getOptions().getDefaultPutOptions());
        System.out.println("\n\n+++NSPO:\n"+syncNSP.getOptions().getDefaultPutOptions());
	}
	
	public static void main(String[] args) {
		Util.runTests(PerspectiveTest.class);
	}

}

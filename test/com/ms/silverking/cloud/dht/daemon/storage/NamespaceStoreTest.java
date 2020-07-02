package com.ms.silverking.cloud.dht.daemon.storage;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Test;

import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.NonExistenceResponse;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.SecondaryTarget;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.WaitMode;
import com.ms.silverking.cloud.dht.client.SimpleTimeoutController;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;
import com.ms.silverking.cloud.dht.common.MetaDataUtil;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.cloud.dht.daemon.ActiveProxyRetrieval;
import com.ms.silverking.cloud.dht.daemon.NodeRingMaster2;
import com.ms.silverking.cloud.dht.net.ForwardingMode;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.dht.serverside.PutTrigger;
import com.ms.silverking.cloud.dht.serverside.RetrieveTrigger;
import com.ms.silverking.cloud.dht.serverside.SSNamespaceStore;
import com.ms.silverking.cloud.dht.serverside.SSRetrievalOptions;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.time.AbsMillisTimeSource;

import mockit.Mock;
import mockit.MockUp;

/**
 * To run this test in the IDE, you must add:
 *   -javaagent:M://dist/ossjava/PROJ/jmockit/1.46.0/lib/jmockit-1.46.jar
 * to VM options. Gradle adds this, so no changes are necessary to run via the
 * command line.
 */
public class NamespaceStoreTest {

    private Pair<PutTrigger, RetrieveTrigger> triggerOverrides = null;

    public NamespaceStore getNS(int version) {
        return getNS(version, true);
    }

    public NamespaceStore getNS(int version, boolean recovery) {
        MessageGroupBase mg = mock(MessageGroupBase.class);
        NodeRingMaster2 ringMaster = mock(NodeRingMaster2.class);
        AbsMillisTimeSource ts = mock(AbsMillisTimeSource.class);
        when(mg.getAbsMillisTimeSource()).thenReturn(ts);
        byte[] rb = new byte[100];

        Arrays.fill(rb, (byte) 1);

        NamespaceProperties nsprop = new NamespaceProperties(DHTConstants.defaultNamespaceOptions.consistencyProtocol(ConsistencyProtocol.LOOSE));
        return new NamespaceStore(1, null, NamespaceStore.DirCreationMode.DoNotCreateNSDir,
                nsprop, mg, ringMaster,
                recovery, new ConcurrentHashMap<UUIDBase, ActiveProxyRetrieval>()) {
            @Override
            protected long newestVersion(DHTKey key) {
                return version;
            }

            @Override
            public ByteBuffer _retrieve(DHTKey key, RetrievalOptions options) {
                ByteBuffer result = mock(ByteBuffer.class);
                when(result.getShort(anyInt())).thenReturn((short) 1500);
                when(result.asReadOnlyBuffer()).thenReturn(ByteBuffer.wrap(rb));
                return result;
            }

            @Override
            protected Pair<PutTrigger, RetrieveTrigger> createTriggers() {
                if (triggerOverrides != null) {
                    return triggerOverrides;
                } else {
                    return super.createTriggers();
                }
            }
        };
    }

    @Test
    public void testSingleVersionSameValue() {
        int version = 0;
        ByteBuffer bb = mock(ByteBuffer.class);
        byte[] checksum = new byte[8];
        Arrays.fill(checksum, (byte) 1);
        NamespaceStore ns = getNS(version);
        StorageParametersAndRequirements storageParams = new StorageParametersAndRequirements(version + 1,
                1, 1, (short) 1500, checksum, new byte[]{1, 1}, 1, PutOptions.noVersionRequired, (short)0);
        assertTrue(ns._put(new SimpleKey(1, 1), bb, storageParams, new byte[]{},
                NamespaceVersionMode.SINGLE_VERSION) == SegmentStorageResult.duplicateStore.toOpResult());
    }

    @Test
    public void testEqualVersionSameValue() {
        int version = 0;
        ByteBuffer bb = mock(ByteBuffer.class);
        byte[] checksum = new byte[8];
        Arrays.fill(checksum, (byte) 1);
        NamespaceStore ns = getNS(version);
        StorageParametersAndRequirements storageParams = new StorageParametersAndRequirements(version,
                1, 1, (short) 1500, checksum, new byte[]{1, 1}, 1, PutOptions.noVersionRequired, (short)0);
        assertTrue(ns._put(new SimpleKey(1, 1), bb, storageParams, new byte[]{},
                NamespaceVersionMode.SINGLE_VERSION) == SegmentStorageResult.duplicateStore.toOpResult());
    }

    @Test
    public void testSingleVersionDifferentValue() {
        int version = 0;
        ByteBuffer bb = mock(ByteBuffer.class);
        byte[] checksum = new byte[8];
        Arrays.fill(checksum, (byte) 0);
        NamespaceStore ns = getNS(version);
        StorageParametersAndRequirements storageParams = new StorageParametersAndRequirements(version + 1,
                1, 1, (short) 1500, checksum, new byte[]{1, 1}, 1, PutOptions.noVersionRequired, (short)0);
        assertTrue(ns._put(new SimpleKey(1, 1), bb, storageParams, new byte[]{},
                NamespaceVersionMode.SINGLE_VERSION) == SegmentStorageResult.mutation.toOpResult());
    }

    @Test
    public void testSmallerVersionNoRevision() {
        int version = 0;
        ByteBuffer bb = mock(ByteBuffer.class);
        byte[] checksum = new byte[8];
        Arrays.fill(checksum, (byte) 1);
        NamespaceStore ns = getNS(version);
        StorageParametersAndRequirements storageParams = new StorageParametersAndRequirements(version - 1,
                1, 1, (short) 1500, checksum, new byte[]{1, 1}, 1, PutOptions.noVersionRequired, (short)0);
        assertTrue(ns._put(new SimpleKey(1, 1), bb, storageParams, new byte[]{},
                NamespaceVersionMode.SINGLE_VERSION) == SegmentStorageResult.invalidVersion.toOpResult());
    }

    private Pair<ByteBuffer, ByteBuffer[]> retrieveInvalidations(DHTKey key,
                                                                 DHTKey[] keys,
                                                                 boolean dataIsInvalid,
                                                                 boolean includeInvalidations) {
        assert (key == null ^ keys == null);
        MessageGroupBase mg = mock(MessageGroupBase.class);
        NodeRingMaster2 ringMaster = mock(NodeRingMaster2.class);
        AbsMillisTimeSource ts = mock(AbsMillisTimeSource.class);
        when(mg.getAbsMillisTimeSource()).thenReturn(ts);

        new MockUp<MetaDataUtil>() {
            @Mock
            public boolean isInvalidated(byte[] storedValue, int baseOffset) {
                return dataIsInvalid;
            }
        };
        new MockUp<MetaDataUtil>() {
            @Mock
            public boolean isInvalidation(ByteBuffer storedValue, int baseOffset) {
                return dataIsInvalid;
            }
        };

        NamespaceProperties nsprop = new NamespaceProperties(DHTConstants.defaultNamespaceOptions.consistencyProtocol(ConsistencyProtocol.LOOSE));
        NamespaceStore ns = new NamespaceStore(1,
                null,
                NamespaceStore.DirCreationMode.DoNotCreateNSDir,
                nsprop,
                mg,
                ringMaster,
                true,
                new ConcurrentHashMap<UUIDBase, ActiveProxyRetrieval>()) {
            @Override
            protected ByteBuffer[] _retrieve(DHTKey[] keys, InternalRetrievalOptions options) {
                ByteBuffer[] bb = new ByteBuffer[keys.length];
                for (int i = 0; i < keys.length; i++) {
                    bb[i] = _retrieve(keys[i], options);
                }
                return bb;
            }

            @Override
            protected ByteBuffer _retrieve(DHTKey key, InternalRetrievalOptions options) {
                ByteBuffer bb = ByteBuffer.allocate(1);
                bb.put((byte) 1);
                return bb;
            }
        };
        RetrievalOptions ro = new RetrievalOptions(new SimpleTimeoutController(1, 0),
                new HashSet<SecondaryTarget>(),
                RetrievalType.META_DATA,
                WaitMode.GET,
                VersionConstraint.defaultConstraint,
                NonExistenceResponse.defaultResponse,
                true,
                includeInvalidations,
                ForwardingMode.DO_NOT_FORWARD,
                false,
                new byte[0],
                new byte[0]);
        SSRetrievalOptions options = new InternalRetrievalOptions(ro, false);

        ByteBuffer result;
        ByteBuffer[] results;

        if (keys == null) {
            result = ns.retrieve(key, options);
            results = null;
        } else {
            result = null;
            results = ns.retrieve(keys, options);
        }
        return new Pair<ByteBuffer, ByteBuffer[]>(result, results);
    }

    // single key tests
    @Test
    public void testRetrieveSingleKeyDoesIncludeInvalidationsWhenDataIsInvalidAndReturnInvalidations() {
        Pair<ByteBuffer, ByteBuffer[]> resultPair = retrieveInvalidations(new SimpleKey(0, 0), null, true, true);
        assertTrue(resultPair.getV1() != null);
    }

    @Test
    public void testRetrieveSingleKeyDoesNotIncludeInvalidationsWhenDataIsInvalidAndNotReturnInvalidations() {
        Pair<ByteBuffer, ByteBuffer[]> resultPair = retrieveInvalidations(new SimpleKey(0, 0), null, true, false);
        assertTrue(resultPair.getV1() == null);
    }

    @Test
    public void testRetrieveSingleKeyDoesIncludeInvalidationsWhenDataIsNotInvalidAndReturnInvalidations() {
        Pair<ByteBuffer, ByteBuffer[]> resultPair = retrieveInvalidations(new SimpleKey(0, 0), null, false, true);
        assertTrue(resultPair.getV1() != null);
    }

    @Test
    public void testRetrieveSingleKeyDoesNotIncludeInvalidationsWhenDataIsNotInvalidAndNotReturnInvalidations() {
        Pair<ByteBuffer, ByteBuffer[]> resultPair = retrieveInvalidations(new SimpleKey(0, 0), null, false, false);
        assertTrue(resultPair.getV1() != null);
    }

    // multiple key tests
    @Test
    public void testRetrieveMultipleKeysDoesIncludeInvalidationsWhenDataIsInvalidAndReturnInvalidations() {
        DHTKey[] keys = new DHTKey[1];
        keys[0] = new SimpleKey(0, 0);
        Pair<ByteBuffer, ByteBuffer[]> resultPair = retrieveInvalidations(null, keys, true, true);
        assertTrue(resultPair.getV2()[0] != null);
    }

    @Test
    public void testRetrieveMultipleKeysDoesNotIncludeInvalidationsWhenDataIsInvalidAndNotReturnInvalidations() {
        DHTKey[] keys = new DHTKey[1];
        keys[0] = new SimpleKey(0, 0);
        Pair<ByteBuffer, ByteBuffer[]> resultPair = retrieveInvalidations(null, keys, true, false);
        assertTrue(resultPair.getV2()[0] == null);
    }

    @Test
    public void testRetrieveMultipleKeysDoesIncludeInvalidationsWhenDataIsNotInvalidAndReturnInvalidations() {
        DHTKey[] keys = new DHTKey[1];
        keys[0] = new SimpleKey(0, 0);
        Pair<ByteBuffer, ByteBuffer[]> resultPair = retrieveInvalidations(null, keys, false, true);
        assertTrue(resultPair.getV2()[0] != null);
    }

    @Test
    public void testRetrieveMultipleKeysDoesNotIncludeInvalidationsWhenDataIsNotInvalidAndNotReturnInvalidations() {
        DHTKey[] keys = new DHTKey[1];
        keys[0] = new SimpleKey(0, 0);
        Pair<ByteBuffer, ByteBuffer[]> resultPair = retrieveInvalidations(null, keys, false, false);
        assertTrue(resultPair.getV2()[0] != null);
    }

    class DummyTrigger implements RetrieveTrigger {

        public boolean hasInitialised = false;

        @Override
        public ByteBuffer retrieve(SSNamespaceStore nsStore, DHTKey key, SSRetrievalOptions options) {
            return null;
        }

        @Override
        public ByteBuffer[] retrieve(SSNamespaceStore nsStore, DHTKey[] keys, SSRetrievalOptions options) {
            return new ByteBuffer[0];
        }

        @Override
        public Iterator<DHTKey> keyIterator() {
            return null;
        }

        @Override
        public long getTotalKeys() {
            return 0;
        }

        @Override
        public boolean subsumesStorage() {
            return false;
        }

        @Override
        public void initialize(SSNamespaceStore nsStore) {
            hasInitialised = true;
        }
    }

    @Test
    public void testTriggersNotInitialisedOnConstructionIfRecovery() {
        DummyTrigger dummy = new DummyTrigger();
        triggerOverrides = new Pair<>(null, dummy);

        // Should not init during construction for recovery
        getNS(0, true);
        assertFalse(dummy.hasInitialised);

        dummy.hasInitialised = false;
        // Should init during construction for new namespace
        getNS(0, false);
        assert(dummy.hasInitialised);
    }
}

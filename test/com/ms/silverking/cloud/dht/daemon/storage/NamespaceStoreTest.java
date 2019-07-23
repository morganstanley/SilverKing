package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.NonExistenceResponse;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.RetrievalOptions;

import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.WaitMode;
import com.ms.silverking.cloud.dht.client.SimpleTimeoutController;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.cloud.dht.daemon.ActiveProxyRetrieval;
import com.ms.silverking.cloud.dht.daemon.NodeRingMaster2;
import com.ms.silverking.cloud.dht.net.ForwardingMode;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.time.AbsMillisTimeSource;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class NamespaceStoreTest {

    public NamespaceStore getNS(int version) {
        File nsDir = mock(File.class);
        MessageGroupBase mg = mock(MessageGroupBase.class);
        NodeRingMaster2 ringMaster = mock(NodeRingMaster2.class);
        AbsMillisTimeSource ts = mock(AbsMillisTimeSource.class);
        when(mg.getAbsMillisTimeSource()).thenReturn(ts);
        byte[] rb = new byte[100];

        Arrays.fill(rb, (byte) 1);


        NamespaceProperties nsprop = new NamespaceProperties(DHTConstants.defaultNamespaceOptions.consistencyProtocol(ConsistencyProtocol.LOOSE));
        return new NamespaceStore(1, null , NamespaceStore.DirCreationMode.DoNotCreateNSDir,
                nsprop , mg, ringMaster,
                true, new ConcurrentHashMap<UUIDBase, ActiveProxyRetrieval>()){
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
                1, 1, (short) 1500, checksum, new byte[]{1, 1}, 1, PutOptions.noVersionRequired );
        assertTrue(ns._put(new SimpleKey(1,1), bb, storageParams, new byte[]{},
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
                1, 1, (short) 1500, checksum, new byte[]{1, 1}, 1, PutOptions.noVersionRequired );
        assertTrue(ns._put(new SimpleKey(1,1), bb, storageParams, new byte[]{},
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
                1, 1, (short) 1500, checksum, new byte[]{1, 1}, 1, PutOptions.noVersionRequired );
        assertTrue(ns._put(new SimpleKey(1,1), bb, storageParams, new byte[]{},
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
                1, 1, (short) 1500, checksum, new byte[]{1, 1}, 1, PutOptions.noVersionRequired);
        assertTrue(ns._put(new SimpleKey(1, 1), bb, storageParams, new byte[]{},
                NamespaceVersionMode.SINGLE_VERSION) == SegmentStorageResult.invalidVersion.toOpResult());
    }

    private class DummyReadThroughPlugin extends ReadThroughPlugin {
        private int readCalls = 0;
        private int putCalls = 0;
        private int putCompletes = 0;
        private ByteBuffer result = ByteBuffer.allocate(10);

        int getReadCalls() {
            return readCalls;
        }

        int getPutCalls() {
            return putCalls;
        }

        int getPutCompletes() {
            return putCompletes;
        }

        void reset() {
            readCalls = 0;
            putCalls = 0;
            putCompletes = 0;
        }

        @Override
        protected void notifyReadThroughBatch(DHTKey[] keys, byte[] userOptions) {
            readCalls += 1;
        }

        @Override
        protected void notifyReadThrough(DHTKey key, byte[] userOptions) {
            readCalls += 1;
        }

        @Override
        protected void notifyPutBatch(DHTKey[] keys, ByteBuffer[] items) {
            putCalls += 1;
        }

        @Override
        protected void notifyPutSingle(DHTKey key, ByteBuffer item) {
            putCalls += 1;
        }

        @Override
        protected void notifyPutComplete(DHTKey key, ByteBuffer item, OpResult res) {
            assertEquals("Put should complete with mutation", OpResult.MUTATION, res);
            putCompletes += 1;
        }

        @Override
        protected ByteBuffer readThroughSingleImpl(DHTKey key, byte[] userOptions) {
            return result;
        }

        @Override
        protected ByteBuffer[] readThroughBatchImpl(DHTKey[] keys, byte[] userOptions) {
            ByteBuffer[] ret = new ByteBuffer[keys.length];
            Arrays.fill(ret, result);
            return ret;
        }

        @Override
        String getName() {
            return "DummyReadThroughPlugin";
        }
    }

    private InternalRetrievalOptions generateRetrievalOptions(RetrievalType retType) {
        RetrievalOptions rOpts;

        rOpts = new RetrievalOptions(new SimpleTimeoutController(1, Integer.MAX_VALUE),
                new HashSet<>(), retType, WaitMode.GET, VersionConstraint.defaultConstraint,
                NonExistenceResponse.NULL_VALUE, false, false,
                ForwardingMode.DO_NOT_FORWARD, false);

        return new InternalRetrievalOptions(rOpts);
    }

    @Test
    public void testNonGroupedReadMayInvokeReadThroughPlugin() {
        NamespaceStore ns = getNS(0);
        DummyReadThroughPlugin dummy = new DummyReadThroughPlugin();
        ns.setReadThroughPlugin(dummy);

        SimpleKey key = SimpleKey.randomKey();
        UUIDBase uuid = new UUIDBase();

        List<DHTKey> keys = new ArrayList<DHTKey>();
        keys.add(key);

        // Should invoke when using VALUE_AND_READ_THROUGH
        InternalRetrievalOptions optsA = generateRetrievalOptions(RetrievalType.VALUE_AND_READ_THROUGH);
        ns.retrieve_nongroupedImpl(keys, optsA, uuid);
        assertEquals("Should have invoked a read through", 1, dummy.getReadCalls());
        assertEquals("Should have invoked a put call", 1, dummy.getPutCalls());
        assertEquals("Should have invoked and completed put call", 1, dummy.getPutCompletes());
        dummy.reset();

        // Should never invoke when not using a read through retrieval type
        InternalRetrievalOptions optsB = generateRetrievalOptions(RetrievalType.VALUE);
        ns.retrieve_nongroupedImpl(keys, optsB, uuid);
        assertEquals("Should not have invoked a read through", 0, dummy.getReadCalls());
        assertEquals("Should not have invoked a put call", 0, dummy.getPutCalls());
        assertEquals("Should not have invoked and completed put call", 0, dummy.getPutCompletes());
    }

    @Test
    public void testGroupedReadMayInvokeReadThroughPlugin() {
        NamespaceStore ns = getNS(0);
        DummyReadThroughPlugin dummy = new DummyReadThroughPlugin();
        ns.setReadThroughPlugin(dummy);

        SimpleKey key = SimpleKey.randomKey();
        UUIDBase uuid = new UUIDBase();

        // the read through happens at the retrieve level regardless of the number of keys
        // so it doesn't matter that this will go to the single key case for a batch
        // the same read through code will be invoked
        List<DHTKey> keys = new ArrayList<DHTKey>();
        keys.add(key);

        // Should invoke when using VALUE_AND_READ_THROUGH
        InternalRetrievalOptions optsA = generateRetrievalOptions(RetrievalType.VALUE_AND_READ_THROUGH);
        ns.retrieve(keys, optsA, uuid);
        assertEquals("Should have invoked a read through", 1, dummy.getReadCalls());
        assertEquals("should have invoked a put call", 1, dummy.getPutCalls());
        assertEquals("Should have invoked and completed put call", 1, dummy.getPutCompletes());
        dummy.reset();

        // Should never invoke when not using a read through retrieval type
        InternalRetrievalOptions optsB = generateRetrievalOptions(RetrievalType.VALUE);
        ns.retrieve(keys, optsB, uuid);
        assertEquals("Should not have invoked a read through", 0, dummy.getReadCalls());
        assertEquals("Should not have invoked read through put", 0, dummy.getPutCalls());
        assertEquals("Should not have invoked and completed put call", 0, dummy.getPutCompletes());
    }

}

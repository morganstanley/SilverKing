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

}

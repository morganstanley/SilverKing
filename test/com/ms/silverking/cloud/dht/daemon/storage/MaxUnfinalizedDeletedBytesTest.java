package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.LRURetentionPolicy;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceServerSideCode;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.NodeRingMaster2;
import com.ms.silverking.cloud.dht.daemon.storage.serverside.LRUTrigger;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.util.jvm.Finalization;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class MaxUnfinalizedDeletedBytesTest {
    private MessageGroupBase mockMg = mock(MessageGroupBase.class);
    private NodeRingMaster2 mockRingMaster = mock(NodeRingMaster2.class);
    private File fakeNsDir = spy(new File("dummy"));
    private FileSegmentCompactor fakeFileSegmentCompactor = spy(new FileSegmentCompactor());
    private Finalization fakeFinalization = spy(new Finalization(SystemTimeUtil.timerDrivenTimeSource, true));

    private NamespaceStore createFakeNS() {
        NamespaceOptions testMutableLruNsOpts = DHTConstants.defaultNamespaceOptions
            .consistencyProtocol(ConsistencyProtocol.LOOSE)
            .versionMode(NamespaceVersionMode.SYSTEM_TIME_NANOS)
            .namespaceServerSideCode(NamespaceServerSideCode.singleTrigger(LRUTrigger.class))
            .valueRetentionPolicy(new LRURetentionPolicy(1000, 1))
            .segmentSize(testSegimentSizeBytes);
        NamespaceProperties nsprop = new NamespaceProperties(testMutableLruNsOpts);

        return new NamespaceStore(
            1,
            fakeNsDir,
            NamespaceStore.DirCreationMode.DoNotCreateNSDir,
            nsprop,
            null,
            mockMg,
            mockRingMaster,
            true,
            new ConcurrentHashMap<>(),
            new LRUReapPolicy(true, true, true, 500, 1000, 1000),
            fakeFinalization
        ) {
            @Override
            protected FileSegmentCompactor createFileSegmentCompactor() {
                return fakeFileSegmentCompactor;
            }
        };
    }


    // Literally disable auto finalization
    private int testMinFinalizationIntervalMillis = Integer.MAX_VALUE;
    // 1000 bytes threshold to trigger finalization
    private int testSegimentSizeBytes = 5000;
    // These numbers mean if there are at least 3 deleted segments, then the forced GC/Finalization
    private int testForcedGcThreshold = 3;
    private long testMaxUnfinalizedDeletedBytes = (long) (testForcedGcThreshold) * (long) (testSegimentSizeBytes) - 1;

    @Before
    public void setUpBefore() {
        System.setProperty(DHTConstants.minFinalizationIntervalMillisProperty, testMinFinalizationIntervalMillis + "");
        System.setProperty(DHTConstants.maxUnfinalizedDeletedBytesProperty, testMaxUnfinalizedDeletedBytes + "");
    }


    @Test
    public void testMaxUnfinalizedDeletedBytes() {
        NamespaceStore fakeNs = spy(createFakeNS());
        doNothing().when(fakeNs).initializeReapImplState();
        // Simulation : 1 segement is deleted each time NS does liveReap()
        doReturn(1).when(fakeFileSegmentCompactor).emptyTrashAndCompaction(fakeNsDir);
        doReturn(false).when(fakeFinalization).forceFinalization(testMinFinalizationIntervalMillis);

        // no any gc before liveReap
        verify(fakeFinalization, times(0)).forceFinalization(0);
        verify(fakeFinalization, times(0)).forceFinalization(testMinFinalizationIntervalMillis);
        for (int i = 1; i < testForcedGcThreshold; i++) {
            fakeNs.liveReap();
            // No forced gc shall be triggered before hitting the threshold
            verify(fakeFinalization, times(0)).forceFinalization(0);
            // Auto/Normal gc shall be called during each iteration before hitting the threshold
            verify(fakeFinalization, times(i)).forceFinalization(testMinFinalizationIntervalMillis);
        }
        // This liveReap will hit the threshold for forced gc
        fakeNs.liveReap();
        verify(fakeFinalization, times(1)).forceFinalization(0);
        verify(fakeFinalization, times(testForcedGcThreshold - 1)).forceFinalization(testMinFinalizationIntervalMillis);
    }
}

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
    private FileSegmentCompactor mockFileSegmentCompactor = mock(FileSegmentCompactor.class);
    private File fakeNsDir = spy(new File("dummy"));
    private Finalization fakeFinalization = spy(new Finalization(SystemTimeUtil.timerDrivenTimeSource, true));

    private NamespaceStore createFakeNS(int testSegmentSizeBytes) {
        NamespaceOptions testMutableLruNsOpts = DHTConstants.defaultNamespaceOptions
            .consistencyProtocol(ConsistencyProtocol.LOOSE)
            .versionMode(NamespaceVersionMode.SYSTEM_TIME_NANOS)
            .namespaceServerSideCode(NamespaceServerSideCode.singleTrigger(LRUTrigger.class))
            .valueRetentionPolicy(new LRURetentionPolicy(1000, 1))
            .segmentSize(testSegmentSizeBytes);
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
            fakeFinalization,
            mockFileSegmentCompactor
        );
    }

    @Test
    public void testMaxUnfinalizedDeletedBytes() {
        int minFinalizationIntervalMillis = NamespaceStore.getMinFinalizationIntervalMillis();
        long maxUnfinalizedDeletedBytes = NamespaceStore.getMaxUnfinalizedDeletedBytes();

        int proposedLoopTime = 5;
        int proposedSegmentNumDeletedEachTime = 1;
        int proposedSegmentSize;
        // adjust for overflow (in case of maxUnfinalizedDeletedBytes has been set to a very crazy number :-)
        long tmp = maxUnfinalizedDeletedBytes / ((long)proposedLoopTime * (long)proposedSegmentNumDeletedEachTime);
        if (tmp  < NamespaceOptions.maxMaxValueSize) {
            proposedSegmentSize = (int)tmp;
        } else {
            // if maxUnfinalizedDeletedBytes is crazily large
            // 1. try to adjust proposedSegmentSize to maximum
            proposedSegmentSize = NamespaceOptions.maxMaxValueSize;
            // 2. try to adjust proposedSegmentNumDeletedEachTime
            long tmp2 = maxUnfinalizedDeletedBytes / ((long)proposedLoopTime * (long)proposedSegmentSize);
            if (tmp2 < Integer.MAX_VALUE) {
                proposedSegmentNumDeletedEachTime = (int)tmp2;
            } else {
                proposedSegmentNumDeletedEachTime = Integer.MAX_VALUE;
                // 3. try to adjust loopTime (will be 4 loops, if both proposedSegmentSize and proposedSegmentNumDeletedEachTime are maximum)
                proposedLoopTime = (int)(maxUnfinalizedDeletedBytes / ((long)proposedSegmentNumDeletedEachTime * (long)proposedSegmentSize));
            }
        }

        NamespaceStore fakeNs = spy(createFakeNS(proposedSegmentSize));
        doNothing().when(fakeNs).initializeReapImplState();
        // Simulation : 1 segement is deleted each time NS does liveReap()
        doReturn(proposedSegmentNumDeletedEachTime).when(mockFileSegmentCompactor).emptyTrashAndCompaction(fakeNsDir);
        doReturn(false).when(fakeFinalization).forceFinalization(minFinalizationIntervalMillis);

        // no any gc before liveReap
        verify(fakeFinalization, times(0)).forceFinalization(0);
        verify(fakeFinalization, times(0)).forceFinalization(minFinalizationIntervalMillis);
        for (int i = 1; i <= proposedLoopTime; i++) {
            fakeNs.liveReap();
            // No forced gc shall be triggered before hitting the threshold
            verify(fakeFinalization, times(0)).forceFinalization(0);
            // Auto/Normal gc shall be called during each iteration before hitting the threshold
            verify(fakeFinalization, times(i)).forceFinalization(minFinalizationIntervalMillis);
        }
        // This liveReap will hit the threshold for forced gc
        fakeNs.liveReap();
        verify(fakeFinalization, times(1)).forceFinalization(0);
        verify(fakeFinalization, times(proposedLoopTime)).forceFinalization(minFinalizationIntervalMillis);
    }
}

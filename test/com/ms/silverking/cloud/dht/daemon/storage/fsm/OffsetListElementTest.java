package com.ms.silverking.cloud.dht.daemon.storage.fsm;

import org.junit.Assert;
import org.junit.Test;

import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.daemon.storage.OffsetList;
import com.ms.silverking.cloud.dht.daemon.storage.OffsetListStore;
import com.ms.silverking.cloud.dht.daemon.storage.RAMOffsetListStore;

public class OffsetListElementTest {
    private static final int    numOffsetLists = 100;
    private static final int    entriesPerList = 10;
    
    @Test
    public void test() {
        RAMOffsetListStore      ols0;
        OffsetListStore         ols1;
        OffsetList[]            offsetLists;
        OffsetListsElement       e;
        
        ols0 = new RAMOffsetListStore(DHTConstants.defaultNamespaceOptions);
        offsetLists = new OffsetList[numOffsetLists];
        for (int i = 0; i < numOffsetLists; i++) {
            offsetLists[i] = ols0.newOffsetList();
            for (int j = 0; j < entriesPerList; j++) {
                offsetLists[i].putOffset(j, j, 0);
            }
        }
        
        e = OffsetListsElement.create(ols0);
        ols1 = e.getOffsetListStore(DHTConstants.defaultNamespaceOptions);
        
        for (int i = 0; i < numOffsetLists; i++) {
            for (int j = 0; j < entriesPerList; j++) {
                int offset;
                
                offset = ols1.getOffsetList(i + 1).getOffset(VersionConstraint.exactMatch(j), null);
                //System.out.printf("%d\t%d\t%d\n", i, j, offset);
                Assert.assertEquals(j, offset);
            }
        }
    }
}

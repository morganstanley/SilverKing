package com.ms.silverking.cloud.dht.daemon.storage.fsm;

import org.junit.Assert;
import org.junit.Test;

import com.ms.silverking.cloud.dht.collection.IntArrayCuckoo;
import com.ms.silverking.cloud.dht.collection.IntBufferCuckoo;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.cloud.dht.daemon.storage.StoreConfiguration;

public class KeyToOffsetMapElementTest {
    private static final int    numKeys = 100;
    
    @Test
    public void test() {
        IntArrayCuckoo          iac;
        IntBufferCuckoo         ibc;
        KeyToOffsetMapElement   e;
        DHTKey[]                keys;
        
        iac = new IntArrayCuckoo(StoreConfiguration.fileInitialCuckooConfig);
        keys = new DHTKey[numKeys];
        for (int i = 0; i < numKeys; i++) {
            keys[i] = SimpleKey.randomKey();
            iac.put(keys[i], keys[i].hashCode());
        }
        e = KeyToOffsetMapElement.create(iac);
        ibc = (IntBufferCuckoo)e.getKeyToOffsetMap();
        for (int i = 0; i < numKeys; i++) {
            //System.out.printf("%s\t%d\t%d\n", KeyUtil.keyToString(keys[i]), ibc.get(keys[i]), keys[i].hashCode());
            Assert.assertEquals(ibc.get(keys[i]), keys[i].hashCode());
        }
    }
}

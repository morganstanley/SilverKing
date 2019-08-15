package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.cloud.dht.daemon.storage.serverside.LRUKeyedInfo;
import com.ms.silverking.cloud.dht.daemon.storage.serverside.LRUStateImpl;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LRUStateTest {
    private DummyTimeSource time;
    private LRUStateImpl state;

    @Before
    public void setup() {
        time = new DummyTimeSource();
        state = new LRUStateImpl(time);
    }

    private <T extends LRUKeyedInfo> List<T> iteratorToList(Iterator<T> it) {
        Iterable<T> iter = () -> (Iterator<T>) state.getLRUList().iterator();
        List<T> lst = StreamSupport
                .stream(iter.spliterator(), false)
                .collect(Collectors.toList());
        return lst;
    }

    @Test public void testSinglePut() {
        DHTKey keyA = SimpleKey.randomKey();

        state.markPut(keyA, 1);

        List<LRUKeyedInfo> listA = iteratorToList(state.getLRUList().iterator());
        assertEquals("LRU List should have one item after put", 1, listA.size());
        LRUKeyedInfo itemA = listA.get(0);
        assertEquals("Key should be maintained", keyA, itemA.getKey());
        assertEquals("Time should be maintained", 0L, itemA.getAccessTime());
    }

    @Test public void testRepeatedPut() {
        DHTKey keyA = SimpleKey.randomKey();

        state.markPut(keyA, 1);

        // update and retouch the key
        time.setTime(1L);
        state.markPut(keyA, 1);

        List<LRUKeyedInfo> list = iteratorToList(state.getLRUList().iterator());
        LRUKeyedInfo item = list.get(0);
        assertEquals("LRU List should have one item after repeated key put", 1, list.size());
        assertEquals("Key should be maintained", keyA, item.getKey());
        assertEquals("Time should be updated by second put", 1L, item.getAccessTime());
    }

    @Test public void testMultiplePut() {
        DHTKey keyA = SimpleKey.randomKey();
        DHTKey keyB = SimpleKey.randomKey();

        state.markPut(keyA, 1);

        // update and retouch the key
        time.setTime(1L);
        state.markPut(keyB, 1);

        List<LRUKeyedInfo> list = iteratorToList(state.getLRUList().iterator());
        assertEquals("LRU List should have two items after new key put", 2, list.size());
        LRUKeyedInfo first = list.get(0);
        LRUKeyedInfo second = list.get(1);

        assertTrue("LRUList should be sorted on put order", first.getAccessTime() > second.getAccessTime());
        assertEquals("KeyB should be the oldest key", keyA, second.getKey());
        assertEquals("KeyA should be the newest key", keyB, first.getKey());
    }

    @Test public void testLRUPutAndRead() {
        DHTKey keyA = SimpleKey.randomKey();

        state.markPut(keyA, 1);

        List<LRUKeyedInfo> list = iteratorToList(state.getLRUList().iterator());
        LRUKeyedInfo item = list.get(0);
        assertEquals("LRU List should have one item after repeated key put", 1, list.size());
        assertEquals("Timestamp should be initial put time", 0L, item.getAccessTime());

        time.setTime(1L);
        state.markRead(keyA);
        List<LRUKeyedInfo> list2 = iteratorToList(state.getLRUList().iterator());
        LRUKeyedInfo item2 = list2.get(0);

        assertEquals("LRU List should have one item after read update", 1, list2.size());
        assertEquals("Timestamp should be read time", 1L, item2.getAccessTime());

    }

    @Test public void testLRUReorderAfterTouch() {
        DHTKey keyA = SimpleKey.randomKey();
        DHTKey keyB = SimpleKey.randomKey();

        state.markPut(keyA, 1);

        // update and retouch the key
        time.setTime(1L);
        state.markPut(keyB, 1);

        List<LRUKeyedInfo> list = iteratorToList(state.getLRUList().iterator());
        assertEquals("LRU List should have two items after new key put", 2, list.size());
        LRUKeyedInfo first = list.get(0);
        LRUKeyedInfo second = list.get(1);
        assertTrue("LRUList should be sorted on put order", first.getAccessTime() > second.getAccessTime());

        time.setTime(2L);
        state.markRead(keyA);

        List<LRUKeyedInfo> list2 = iteratorToList(state.getLRUList().iterator());
        assertEquals("LRU List should still have two items after read", 2, list2.size());
        LRUKeyedInfo first2 = list2.get(0);
        assertEquals("KeyA should be newest key once reused", keyA, first2.getKey());
    }

}

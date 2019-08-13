package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.cloud.dht.daemon.storage.serverside.LRUKeyedInfo;
import com.ms.silverking.cloud.dht.daemon.storage.serverside.LRUStateImpl;
import com.ms.silverking.time.AbsNanosTimeSource;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LRUStateTest {

    private class DummyTimeSource implements AbsNanosTimeSource {
        private long time = 0L;

        public void setTime(long newTime) {
            time = newTime;
        }

        @Override
        public long getNanosOriginTime() {
            return 0L;
        }

        @Override
        public long absTimeNanos() {
            return time;
        }

        @Override
        public long relNanosRemaining(long absDeadlineNanos) {
            return 0L;
        }
    }

    private DummyTimeSource time;
    private LRUStateImpl state;

    @Before
    public void setup() {
        time = new DummyTimeSource();
        state = new LRUStateImpl(time);
    }

    @Test public void testSinglePut() {
        DHTKey keyA = SimpleKey.randomKey();

        state.markPut(keyA, 1);

        Iterable<LRUKeyedInfo> it = () -> state.getLRUList().iterator();
        List<LRUKeyedInfo> listA = StreamSupport
                .stream(it.spliterator(), false)
                .collect(Collectors.toList());
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

        Iterable<LRUKeyedInfo> it = () -> state.getLRUList().iterator();
        List<LRUKeyedInfo> list = StreamSupport
                .stream(it.spliterator(), false)
                .collect(Collectors.toList());
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

        Iterable<LRUKeyedInfo> it = () -> state.getLRUList().iterator();
        List<LRUKeyedInfo> list = StreamSupport
                .stream(it.spliterator(), false)
                .collect(Collectors.toList());
        assertEquals("LRU List should have two items after new key put", 2, list.size());
        LRUKeyedInfo first = list.get(0);
        LRUKeyedInfo second = list.get(1);

        assertTrue("LRUList should be sorted on put order", first.getAccessTime() < second.getAccessTime());
        assertEquals("KeyA should be the oldest key", keyA, first.getKey());
        assertEquals("KeyB should be the newest key", keyB, second.getKey());
    }

    @Test public void testLRUPutAndRead() {
        DHTKey keyA = SimpleKey.randomKey();

        state.markPut(keyA, 1);

        Iterable<LRUKeyedInfo> it = () -> state.getLRUList().iterator();
        List<LRUKeyedInfo> list = StreamSupport
                .stream(it.spliterator(), false)
                .collect(Collectors.toList());
        LRUKeyedInfo item = list.get(0);
        assertEquals("LRU List should have one item after repeated key put", 1, list.size());
        assertEquals("Timestamp should be initial put time", 0L, item.getAccessTime());

        time.setTime(1L);
        state.markRead(keyA);
        Iterable<LRUKeyedInfo> it2 = () -> state.getLRUList().iterator();
        List<LRUKeyedInfo> list2 = StreamSupport
                .stream(it2.spliterator(), false)
                .collect(Collectors.toList());
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

        Iterable<LRUKeyedInfo> it = () -> state.getLRUList().iterator();
        List<LRUKeyedInfo> list = StreamSupport
                .stream(it.spliterator(), false)
                .collect(Collectors.toList());
        assertEquals("LRU List should have two items after new key put", 2, list.size());
        LRUKeyedInfo first = list.get(0);
        LRUKeyedInfo second = list.get(1);
        assertTrue("LRUList should be sorted on put order", first.getAccessTime() < second.getAccessTime());

        time.setTime(2L);
        state.markRead(keyA);

        Iterable<LRUKeyedInfo> it2 = () -> state.getLRUList().iterator();
        List<LRUKeyedInfo> list2 = StreamSupport
                .stream(it2.spliterator(), false)
                .collect(Collectors.toList());
        assertEquals("LRU List should still have two items after read", 2, list2.size());
        LRUKeyedInfo first2 = list2.get(0);
        LRUKeyedInfo second2 = list2.get(1);
        assertEquals("KeyB should be newest KeyA once reused", keyB, first2.getKey());
    }

}

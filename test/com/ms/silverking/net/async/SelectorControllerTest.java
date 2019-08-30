package com.ms.silverking.net.async;

import com.ms.silverking.thread.lwt.BaseWorker;
import com.ms.silverking.thread.lwt.LWTPool;
import com.ms.silverking.thread.lwt.LWTPoolParameters;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;

import static org.junit.Assert.*;

public class SelectorControllerTest {

    private LWTPool pool = LWTPoolProvider.createPool(LWTPoolParameters.create("TestPool"));

    @Test
    public void testRunClosesSelector() {
        BaseWorker<Connection> worker = new BaseWorker<Connection>(pool, false, 1, 1) {
            @Override
            public void doWork(Connection item) {
            }
        };
        try {
            SelectorController<Connection> sc = new SelectorController<>(
                    new BaseWorker<ServerSocketChannel>(pool, false, 1, 1) {
                        @Override
                        public void doWork(ServerSocketChannel item) {
                        }
                    },
                    worker,
                    worker,
                    worker,
                    "SelectorControllerTest",
                    1,
                    false
            );
            Thread t = new Thread() {
                @Override
                public void run() {
                    sc.run();
                }
            };
            t.start();
            assertTrue(sc.getSelector().isOpen());
            sc.shutdown();
            assertFalse(sc.getSelector().isOpen());
        } catch (IOException e) {
            assertEquals(1, 0);
        }
    }
}
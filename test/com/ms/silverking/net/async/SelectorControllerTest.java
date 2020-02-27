package com.ms.silverking.net.async;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;

import org.junit.Test;

import com.ms.silverking.collection.Triple;
import com.ms.silverking.thread.lwt.BaseWorker;
import com.ms.silverking.thread.lwt.LWTPool;
import com.ms.silverking.thread.lwt.LWTPoolParameters;
import com.ms.silverking.thread.lwt.LWTPoolProvider;

public class SelectorControllerTest {

    private LWTPool pool = LWTPoolProvider.createPool(LWTPoolParameters.create("TestPool"));

    @Test
    public void testRunClosesSelector() throws IOException, InterruptedException {
        BaseWorker<Connection> worker = new BaseWorker<Connection>(pool, false, 1, 1) {
            @Override
            public void doWork(Connection item) {
            }
        };
        SelectorController<Connection> sc = new SelectorController<>(
                new BaseWorker<Triple<ServerSocketChannel,SelectorController,SelectionKey>>(pool, false, 1, 1) {
                    @Override
                    public void doWork(Triple<ServerSocketChannel,SelectorController,SelectionKey> item) {
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
        Integer i = 0;
        while (i < 10) {
            if (!sc.getSelector().isOpen()) {
                Thread.sleep(100);
            }
            i += 1;
        }
        assertTrue(sc.getSelector().isOpen());
        sc.shutdown();
        i = 0;
        while (i < 10) {
            if (sc.getSelector().isOpen()) {
                Thread.sleep(100);
            }
            i += 1;
        }
        assertFalse(sc.getSelector().isOpen());
    }
}
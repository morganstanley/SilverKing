package com.ms.silverking.net.test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;

import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.HostAndPort;
import com.ms.silverking.net.async.AsyncSendListener;
import com.ms.silverking.net.async.IncomingBufferedData;
import com.ms.silverking.net.async.PersistentAsyncServer;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

public class ByteBufferNetTest implements BufferedDataReceiver, AsyncSendListener {
    private final PersistentAsyncServer<BufferedDataConnection> paServer;
    private final Mode  mode;
    private final Semaphore semaphore;
    private final UUIDBase uuid;
    
    private enum Mode {Server, Client};
    
    public ByteBufferNetTest(Mode mode, int port) throws IOException {
        this.mode = mode;
        paServer = new PersistentAsyncServer<>(port, new BufferedDataConnectionCreator(this));
        semaphore = new Semaphore(0);
        uuid = new UUIDBase(0, 0);
        paServer.enable();
    }
    
    @Override
    public void receive(ByteBuffer[] bufferedData, BufferedDataConnection connection) {
        switch (mode) {
        case Server:
            serverReceive(bufferedData, connection);
            break;
        case Client:
            clientReceive(bufferedData, connection);
            break;
        default: throw new RuntimeException("panic");
        }
    }
    
    private void serverReceive(ByteBuffer[] bufferedData, BufferedDataConnection connection) {
        Log.info("Sending");
        paServer.sendAsynchronous(connection.getRemoteSocketAddress(), bufferedData, uuid, this, Long.MAX_VALUE);
    }
    
    private void clientReceive(ByteBuffer[] bufferedData, BufferedDataConnection connection) {
        Log.info("Received");
        semaphore.release();
    }
    
    private static final byte[] one = {0x00, 0x00, 0x00, 0x01};
    
    private ByteBuffer[] createData() {
        ByteBuffer[]    data;
        
        data = new ByteBuffer[3];
        data[0] = ByteBuffer.wrap(IncomingBufferedData.preamble);
        data[1] = ByteBuffer.wrap(one);
        data[2] = ByteBuffer.wrap("Hello world!".getBytes());
        return data;
    }
    
    public void runClient(HostAndPort serverAddr, double duration) {
        Stopwatch           sw;
        ByteBuffer[]        data;

        data = createData();
        sw = new SimpleStopwatch();
        do {
            //UUIDBase    uuid;
            
            //uuid = new UUIDBase();
            Log.info("Sending");
            try {
                paServer.sendAsynchronous(serverAddr.toInetSocketAddress(), data, uuid, this, Long.MAX_VALUE);
            } catch (UnknownHostException uhe) {
                throw new RuntimeException(uhe);
            }
            Log.info("Sent. Waiting for receive");
            try {
                semaphore.acquire();
            } catch (InterruptedException ie) {
            }
        } while (sw.getSplitSeconds() < duration);
    }
        
    // AsyncSendListener
    
    @Override
    public void sent(UUIDBase uuid) {
        Log.fine("Sent: ", uuid);
    }

    @Override
    public void failed(UUIDBase uuid) {
        Log.fine("Failed: ", uuid);
    }

    @Override
    public void timeout(UUIDBase uuid) {
        Log.fine("Timeout: ", uuid);
    }
    
    //
    
    public static void displayUsage() {
        System.out.println("Client <serverhost:port> <duration>");
        System.out.println("or");
        System.out.println("Server port <duration>");
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            if (args.length != 3) {
                displayUsage();
                return;
            } else {
                Mode    mode;
                double  durationSeconds;
             
                Log.setLevel(Level.ALL);
                LWTPoolProvider.createDefaultWorkPools();
                mode = Mode.valueOf(args[0]);
                durationSeconds = Double.parseDouble(args[2]);
                switch (mode) {
                case Server:
                    if (args.length != 3) {
                        displayUsage();
                    } else {
                        ByteBufferNetTest   bbnTest;
                        int port;
                        
                        port = Integer.parseInt(args[1]);
                        bbnTest = new ByteBufferNetTest(mode, port);
                        ThreadUtil.sleepSeconds(durationSeconds);
                    }
                    break;
                case Client:
                    if (args.length != 3) {
                        displayUsage();
                    } else {
                        ByteBufferNetTest   bbnTest;
                        HostAndPort         serverAddr;
                        
                        serverAddr = new HostAndPort(args[1]);
                        bbnTest = new ByteBufferNetTest(mode, 0);
                        bbnTest.runClient(serverAddr, durationSeconds);
                    }
                    break;
                default: throw new RuntimeException("panic");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

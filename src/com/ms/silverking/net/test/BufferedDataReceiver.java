package com.ms.silverking.net.test;

import java.nio.ByteBuffer;

public interface BufferedDataReceiver {
    public void receive(ByteBuffer[] bufferedData, BufferedDataConnection connection);
}

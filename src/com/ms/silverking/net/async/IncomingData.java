package com.ms.silverking.net.async;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public interface IncomingData {
    public ReadResult readFromChannel(SocketChannel channel) throws IOException;
    public int getLastNumRead();
}

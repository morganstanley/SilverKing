package com.ms.silverking.cloud.argus;

import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.Arrays;

import com.ms.silverking.io.FileUtil;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.thread.ThreadUtil;

public class PeerWarningModule {
  private final int port;
  private final DatagramSocket socket;
  private final File warningFile;
  private final PeerWarningListener listener;
  private boolean running;

  public enum Mode {Server, Client}

  ;

  private static final InetAddress broadcastAddr;
  private static final byte[] warningMessage = "ArgusPeerWarning".getBytes();

  static {
    InetAddress _broadcastAddr;

    try {
      _broadcastAddr = InetAddress.getByName("255.255.255.255");
    } catch (IOException ioe) {
      _broadcastAddr = null;
      Log.logErrorWarning(ioe);
    }
    broadcastAddr = _broadcastAddr;
  }

  public PeerWarningModule(int port, PeerWarningListener listener, File warningFile, int warningFileIntervalSeconds)
      throws SocketException {
    assert listener != null;
    running = true;
    this.port = port;
    if (port != 0) {
      socket = new DatagramSocket(port);
      new UDPListener();
    } else {
      socket = null;
    }
    this.warningFile = warningFile;
    if (warningFile != null) {
      if (warningFileIntervalSeconds < 0) {
        throw new RuntimeException("warningFileIntervalSeconds < 0");
      }
      Log.warning("Creating WarningFileWatcher %s", warningFile);
      new WarningFileWatcher(warningFileIntervalSeconds);
    }
    this.listener = listener;
  }

  class UDPListener implements Runnable {
    UDPListener() {
      new Thread(this).start();
    }

    public void run() {
      DatagramPacket p;
      byte[] receiveBuf;

      receiveBuf = new byte[warningMessage.length];
      p = new DatagramPacket(receiveBuf, receiveBuf.length);
      while (running) {
        try {
          Arrays.fill(receiveBuf, (byte) 0);
          socket.receive(p);
          if (Arrays.equals(receiveBuf, warningMessage)) {
            Log.infof("Received peer warning via UDP from %s", p.getAddress());
            listener.peerWarning();
          } else {
            Log.infof("Ignoring unknown message from %s", p.getAddress());
          }
        } catch (Exception e) {
          Log.fine(e);
          ThreadUtil.pauseAfterException();
        }
      }
    }
  }

  class WarningFileWatcher implements Runnable {
    private final int warningFileIntervalMillis;

    WarningFileWatcher(int warningFileIntervalSeconds) {
      this.warningFileIntervalMillis = warningFileIntervalSeconds * 1000;
      new Thread(this).start();
    }

    public void run() {
      ThreadUtil.randomSleep(0, warningFileIntervalMillis);
      while (running) {
        try {
          ThreadUtil.sleep(warningFileIntervalMillis);
          checkFile();
        } catch (Exception e) {
          Log.fine(e);
          ThreadUtil.pauseAfterException();
        }
      }
    }

    private void checkFile() throws IOException {
      Log.infof("Checking %s", warningFile);
      if (warningFile.exists()) {
        String[] toks;
        String ip;
        long millis;

        toks = FileUtil.readFileAsString(warningFile).split("\t");
        ip = toks[0];
        millis = Long.parseLong(toks[1]);
        if (ip != null && !ip.equals(IPAddrUtil.localIPString())) {
          if (System.currentTimeMillis() - millis < Argus.peerWarningResponseIntervalMillis) {
            Log.infof("Received peer warning via file from %s", ip);
            sendUDPWarning(); // relay to local broadcast domain
            listener.peerWarning();
          }
        }
      }
    }
  }

  private void sendUDPWarning() {
    if (broadcastAddr != null && socket != null) {
      try {
        DatagramPacket p;

        p = new DatagramPacket(warningMessage, warningMessage.length, broadcastAddr, port);
        Log.infof("Sending %s", p);
        socket.send(p);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public void warnPeers() throws IOException {
    sendUDPWarning();
    if (warningFile != null) {
      FileUtil.writeToFile(warningFile, IPAddrUtil.localIPString() + "\t" + System.currentTimeMillis());
    }
  }
}

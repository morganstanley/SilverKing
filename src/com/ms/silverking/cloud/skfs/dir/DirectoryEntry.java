package com.ms.silverking.cloud.skfs.dir;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.skfs.dir.serverside.ByteString;

public interface DirectoryEntry {
  public short getMagic();

  public short getNameLength();

  public short getStatus();

  public long getVersion();

  public String getName();

  public byte[] getNameAsBytes();

  public ByteString getNameAsByteString();

  public ByteBuffer getNameAsByteBuffer();
}

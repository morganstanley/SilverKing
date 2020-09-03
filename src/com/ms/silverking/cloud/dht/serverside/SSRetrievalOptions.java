package com.ms.silverking.cloud.dht.serverside;

import java.util.Optional;

import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.VersionConstraint;

public interface SSRetrievalOptions {
  public boolean getVerifyIntegrity();

  public RetrievalType getRetrievalType();

  public VersionConstraint getVersionConstraint();

  public boolean getReturnInvalidations();

  public byte[] getUserOptions();

  public Optional<byte[]> getTraceID();

  public byte[] getAuthorizationUser();
}

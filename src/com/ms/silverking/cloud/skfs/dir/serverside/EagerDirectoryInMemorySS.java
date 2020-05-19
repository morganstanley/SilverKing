package com.ms.silverking.cloud.skfs.dir.serverside;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.daemon.storage.StorageParameters;
import com.ms.silverking.cloud.dht.serverside.SSRetrievalOptions;
import com.ms.silverking.cloud.dht.serverside.SSStorageParameters;
import com.ms.silverking.cloud.dht.serverside.SSUtil;
import com.ms.silverking.cloud.skfs.dir.DirectoryBase;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.log.Log;

/**
 * Eager extension of BaseDirectoryInMemorySS. Creates serialized directories on write.
 */
public class EagerDirectoryInMemorySS extends BaseDirectoryInMemorySS {
  static {
    fileDeletionWorker = DirectoryServer.fileDeletionWorker;
  }

  EagerDirectoryInMemorySS(DHTKey dirKey, DirectoryBase d, SSStorageParameters storageParams, File sDir,
      NamespaceOptions nsOptions, boolean reap) {
    super(dirKey, d, storageParams, sDir, nsOptions, reap, true);
  }

  public EagerDirectoryInMemorySS(DHTKey dirKey, DirectoryBase d, SSStorageParameters storageParams, File sDir,
      NamespaceOptions nsOptions) {
    this(dirKey, d, storageParams, sDir, nsOptions, true);
  }

  public void update(DirectoryBase update, SSStorageParameters sp) {
    Pair<SSStorageParameters, byte[]> sd;

    this.latestUpdateSP = sp;
    update(update);
    sd = serializeDir();
    //persist(sd.getV1(), sd.getV2());
    serializedVersions.put(sp.getVersion(), new SerializedDirectory(sd, false));
    if (reapTimer.hasExpired() || serializedVersions.size() > reapMaxVersions) {
      List<File> emptyList;

      emptyList = reap();
      if (emptyList.size() != 0) {
        // eager update reap sends to deletion worker itself
        throw new RuntimeException("panic");
      }
      reapTimer.reset();
    }
  }

  public ByteBuffer retrieve(SSRetrievalOptions options) {
    ByteBuffer rVal;
    VersionConstraint vc;
    SerializedDirectory sd;
    Pair<SSStorageParameters, ByteBuffer> sdp;

    vc = options.getVersionConstraint();
    if (vc.equals(VersionConstraint.greatest)) {
      sd = serializedVersions.get(latestUpdateSP.getVersion());
    } else {
      Map.Entry<Long, SerializedDirectory> entry;

      // For directories, versions must be ascending, max creation time not allowed in vc
      if (vc.getMode().equals(VersionConstraint.Mode.GREATEST)) {
        entry = serializedVersions.floorEntry(vc.getMax());
        if (entry != null && entry.getKey() < vc.getMin()) {
          entry = null;
        }
      } else {
        entry = serializedVersions.ceilingEntry(vc.getMin());
        if (entry != null && entry.getKey() > vc.getMax()) {
          entry = null;
        }
      }
      if (entry != null) {
        sd = entry.getValue();
      } else {
        sd = null;
      }
    }
    if (sd != null) {
      try {
        sdp = sd.readDir();

        rVal = SSUtil.retrievalResultBufferFromValue(sdp.getV2(),
            StorageParameters.fromSSStorageParameters(sdp.getV1()), options);
        return rVal;
      } catch (IOException ioe) {
        Log.logErrorWarning(ioe);
      }
    }
    return null;
  }
}

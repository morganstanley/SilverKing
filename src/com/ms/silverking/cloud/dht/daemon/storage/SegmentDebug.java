package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.io.IOException;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.common.NamespaceOptionsMode;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;

public class SegmentDebug {
  private final File nsDir;

  public SegmentDebug(File nsDir) {
    this.nsDir = nsDir;
  }

  public void debug(int segmentNumber, NamespaceOptions nsOptions) throws IOException {
    FileSegment fileSegment;

    fileSegment = FileSegment.openReadOnly(nsDir, segmentNumber, nsOptions.getSegmentSize(), nsOptions);
    fileSegment.displayForDebug();
    // FUTURE - add ability to debug single key
    //ByteBuffer buf = fileSegment.retrieve(new SimpleKey(0x6df75ca5ca56b001L, 0x349088631a803dbeL), new
    // InternalRetrievalOptions(new RetrievalOptions(new SimpleTimeoutController(1, 1000), new HashSet<>(),
    // RetrievalType.VALUE_AND_META_DATA, WaitMode.GET, VersionConstraint.greatest, NonExistenceResponse.EXCEPTION,
    // false, false, ForwardingMode.DO_NOT_FORWARD, false)));
    //System.out.println(buf);
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    // This tool runs stand-alone with dependency of "properties" file, which only used by NSP mode for now
    if (DHTConfiguration.defaultNamespaceOptionsMode != NamespaceOptionsMode.MetaNamespace) {
      throw new IllegalArgumentException(
          "You're in the default mode of [" + DHTConfiguration.defaultNamespaceOptionsMode + "], which is not " +
              "supported by this tool");
    }

    try {
      if (args.length != 2) {
        System.out.println("args: <nsDir> <segmentNumber>");
      } else {
        File nsDir;
        int segmentNumber;
        NamespaceOptions nsOptions;

        nsDir = new File(args[0]);
        if (!nsDir.isDirectory()) {
          throw new RuntimeException("Can't find nsDir: " + nsDir);
        }
        segmentNumber = Integer.parseInt(args[1]);
        nsOptions = NamespacePropertiesIO.read(nsDir).getOptions();
        System.out.println(nsOptions);
        new SegmentDebug(nsDir).debug(segmentNumber, nsOptions);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

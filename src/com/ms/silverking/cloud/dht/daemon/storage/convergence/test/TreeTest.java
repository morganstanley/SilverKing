package com.ms.silverking.cloud.dht.daemon.storage.convergence.test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.daemon.storage.KeyAndVersionChecksum;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ChecksumNode;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.RegionTreeBuilder;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.TreeMatcher;
import com.ms.silverking.cloud.dht.net.ProtoChecksumTreeMessageGroup;
import com.ms.silverking.cloud.ring.RingRegion;

public class TreeTest {
  private final int numKeys;
  private final List<KeyAndVersionChecksum> kvcList;
  private final RingRegion region;

  private static final int entriesPerNode = 10;

  public TreeTest(int numKeys) {
    this.numKeys = numKeys;
    region = new RingRegion(1, 1000000);
    kvcList = new ArrayList<>();
    for (int i = 0; i < numKeys; i++) {
      kvcList.add(new KeyAndVersionChecksum(KeyUtil.randomRegionKey(region), 0, 0));
    }
    Collections.sort(kvcList);
  }

  public void test() {
    ChecksumNode tree1;
    ChecksumNode tree2;
    ChecksumNode tree2b;
    ChecksumNode tree3;
    ChecksumNode tree4;
    ChecksumNode tree5;

    tree1 = RegionTreeBuilder.build(region, entriesPerNode, numKeys, kvcList);
    System.out.println(tree1);
    tree2 = tree1.duplicate();

    System.out.println("\n *** Should be equal ***");
    System.out.println(TreeMatcher.match(tree2, tree1));

    tree2b = RegionTreeBuilder.build(region, entriesPerNode, numKeys / 2, kvcList);
    System.out.println(tree2b);
    System.out.println("\n *** Should be equal (different key estimates) ***");
    System.out.println(TreeMatcher.match(tree2b, tree1));

    List<KeyAndVersionChecksum> _kvcList;
    List<KeyAndVersionChecksum> _kvcList4;
    KeyAndVersionChecksum kvc1;
    KeyAndVersionChecksum kvc2;
    long checksum2;

    // remove one key so that we have destNotInSource and sourceNotInDest tests
    _kvcList = new ArrayList<>(kvcList);
    _kvcList.remove(0);
    tree3 = RegionTreeBuilder.build(region, entriesPerNode, numKeys, _kvcList);

    // now create a mismatch
    _kvcList4 = new ArrayList<>(kvcList);
    kvc1 = _kvcList4.remove(0);
    checksum2 = kvc1.getVersionChecksum() + 1;
    kvc2 = new KeyAndVersionChecksum(kvc1.getKey(), checksum2, 0);
    _kvcList4.add(0, kvc2);
    tree4 = RegionTreeBuilder.build(region, entriesPerNode, numKeys, _kvcList4);

    _kvcList4.remove(1);
    tree5 = RegionTreeBuilder.build(region, entriesPerNode, numKeys, _kvcList4);

    System.out.println("\n *** Should have destNotInSource ***");
    System.out.println(TreeMatcher.match(tree3, tree1));
    System.out.println("\n *** Should have sourceNotInDest ***");
    System.out.println(TreeMatcher.match(tree1, tree3));
    System.out.println("\n *** Should have mismatch ***");
    System.out.println(TreeMatcher.match(tree4, tree1));
    System.out.println("\n *** Should be a mix ***");
    System.out.println(TreeMatcher.match(tree5, tree3));
    System.out.flush();

    serializationTest(tree1);
  }

  private void serializationTest(ChecksumNode checksumNode) {
    ChecksumNode dNode;
    ByteBuffer buffer;

    buffer = ByteBuffer.allocate(1024 * 1024);
    ProtoChecksumTreeMessageGroup.serialize(buffer, checksumNode);
    System.out.println(buffer);
    buffer.flip();
    System.out.println(buffer);
    dNode = ProtoChecksumTreeMessageGroup.deserialize(buffer);
    System.out.println(" *** Pre-serialization ***");
    System.out.println(checksumNode);
    System.out.println(" *** Post-serialization ***");
    System.out.println(dNode);

    System.out.println(" *** Match test ***");
    System.out.println(TreeMatcher.match(dNode, checksumNode));
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      if (args.length != 1) {
        System.out.println("args: <numKeys>");
        return;
      } else {
        TreeTest test;
        int numTests;

        test = new TreeTest(Integer.parseInt(args[0]));
        numTests = 1;
        for (int i = 0; i < numTests; i++) {
          test.test();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

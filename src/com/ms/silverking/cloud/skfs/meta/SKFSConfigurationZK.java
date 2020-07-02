package com.ms.silverking.cloud.skfs.meta;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.management.MetaToolModuleBase;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.io.StreamParser;

public class SKFSConfigurationZK extends MetaToolModuleBase<SKFSConfiguration, MetaPaths> {
  public SKFSConfigurationZK(MetaClient mc) throws KeeperException {
    super(mc, mc.getMetaPaths().getConfigPath());
  }

  @Override
  public SKFSConfiguration readFromFile(File file, long version) throws IOException {
    com.ms.silverking.cloud.skfs.meta.MetaClient skfsMc = (com.ms.silverking.cloud.skfs.meta.MetaClient) (this.mc);
    return new SKFSConfiguration(skfsMc.getSKFSConfigName(), StreamParser.parseLines(file), version, 0L);

  }

  @Override
  public SKFSConfiguration readFromZK(long version, MetaToolOptions options) throws KeeperException {
    com.ms.silverking.cloud.skfs.meta.MetaClient skfsMc = (com.ms.silverking.cloud.skfs.meta.MetaClient) (this.mc);
    return SKFSConfiguration.parse(skfsMc.getSKFSConfigName(), zk.getString(getVBase(version)), version);
  }

  @Override
  public void writeToFile(File file, SKFSConfiguration instance) throws IOException {
    if (file != null) {
      FileOutputStream fs;
      fs = new FileOutputStream(file);
      fs.write(instance.toString().getBytes());
      fs.flush();
      fs.close();
    } else {
      System.out.println(instance.toString());
    }
  }

  @Override
  public String writeToZK(SKFSConfiguration dhtSKFSConfig, MetaToolOptions options)
      throws IOException, KeeperException {
    String path;

    path = zk.createString(base + "/", dhtSKFSConfig.toString(), CreateMode.PERSISTENT_SEQUENTIAL);
    return path;
  }
}

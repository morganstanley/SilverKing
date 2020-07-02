package com.ms.silverking.cloud.dht.meta;

import com.ms.silverking.cloud.management.MetaToolModuleBase;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.io.FileUtil;
import com.ms.silverking.io.StreamParser;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.io.File;
import java.io.IOException;

public class IpAliasConfigurationZk extends MetaToolModuleBase<IpAliasConfiguration, MetaPaths> {

  public IpAliasConfigurationZk(MetaClient mc) throws KeeperException {
    super(mc, MetaPaths.getIpAliasesBase());
  }

  @Override
  public IpAliasConfiguration readFromFile(File file, long version) throws IOException {
    return IpAliasConfiguration.parse(StreamParser.parseLine(file), version);
  }

  private void verifyOptions(MetaToolOptions options) {
    if (options == null) {
      throw new RuntimeException("options == null");
    } else {
      if (options.name == null) {
        throw new RuntimeException("options.name == null");
      } else {
        if (options.name.trim().length() == 0) {
          throw new RuntimeException("options.name.trim().length() == 0");
        } else {
          if (options.name.trim().endsWith("/")) {
            throw new RuntimeException("options.name.trim().endsWith(\"/\")");
          }
        }
      }
    }
  }

  @Override
  public IpAliasConfiguration readFromZK(long version, MetaToolOptions options) throws KeeperException {
    String base;
    String vBase;

    verifyOptions(options);
    base = getBase() + "/" + options.name;
    if (version < 0) {
      version = mc.getZooKeeper().getLatestVersion(base);
    }
    vBase = getVBase(options.name, version);
    return IpAliasConfiguration.parse(zk.getString(vBase), version);
  }

  @Override
  public void writeToFile(File file, IpAliasConfiguration instance) throws IOException {
    FileUtil.writeToFile(file, instance.toString());
  }

  @Override
  public String writeToZK(IpAliasConfiguration instance, MetaToolOptions options) throws IOException, KeeperException {
    verifyOptions(options);
    zk.ensureCreated(base + "/" + options.name);
    return zk.createString(base + "/" + options.name +"/", instance.toString(), CreateMode.PERSISTENT_SEQUENTIAL);
  }
}

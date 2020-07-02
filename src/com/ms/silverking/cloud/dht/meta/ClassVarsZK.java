package com.ms.silverking.cloud.dht.meta;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.management.MetaToolModuleBase;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.io.FileUtil;

public class ClassVarsZK extends MetaToolModuleBase<ClassVars, MetaPaths> {
  public ClassVarsZK(MetaClient mc) throws KeeperException {
    super(mc, mc.getMetaPaths().getClassVarsBasePath());
  }

  @Override
  public ClassVars readFromFile(File file, long version) throws IOException {
    return ClassVars.parse(file, version);
  }

  public ClassVars getClassVars(String classVarsName) throws KeeperException {
    MetaToolOptions mto;

    mto = new MetaToolOptions();
    mto.name = classVarsName;
    return readFromZK(-1, mto);
  }

  @Override
  public ClassVars readFromZK(long version, MetaToolOptions options) throws KeeperException {
    String base;
    String vBase;

    base = getBase() + "/" + options.name;
    if (version < 0) {
      version = mc.getZooKeeper().getLatestVersion(base);
    }
    vBase = getVBase(options.name, version);
    return ClassVars.parse(zk.getString(vBase), version);
  }

  @Override
  public void writeToFile(File file, ClassVars instance) throws IOException {

    FileUtil.writeToFile(file, instance.toString());

  }

  @Override
  public String writeToZK(ClassVars classVars, MetaToolOptions options) throws IOException, KeeperException {
    return writeToZK(classVars, options.name);
  }

  public String writeToZK(ClassVars classVars, String name) throws KeeperException {
    String path;
    //FIXME: temporary work-around until versioning support added
    String classVarsName = base + "/" + name;
    if (zk.exists(classVarsName)) {
      List<String> versions = zk.getChildren(classVarsName);
      for (String ver : versions) {
        zk.delete(classVarsName + "/" + ver);
      }
      zk.delete(classVarsName);
    }
    zk.createString(classVarsName, classVars.toString(), CreateMode.PERSISTENT);
    path = zk.createString(classVarsName + "/", classVars.toString(), CreateMode.PERSISTENT_SEQUENTIAL);
    return path;
  }
}

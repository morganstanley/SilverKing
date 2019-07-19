package com.ms.silverking.cloud.storagepolicy;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.management.MetaToolModuleBase;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.cloud.meta.MetaClientBase;
import com.ms.silverking.cloud.toporing.meta.MetaPaths;

public class StoragePolicyZK extends MetaToolModuleBase<StoragePolicyGroup, MetaPaths> {

    public StoragePolicyZK(MetaClientBase<MetaPaths> mc) throws KeeperException {
        super(mc, mc.getMetaPaths().getStoragePolicyGroupPath());
    }

    @Override
    public StoragePolicyGroup readFromFile(File file, long version) throws IOException {
        return new PolicyParser().parsePolicyGroup(file, version);
    }

    @Override
    public StoragePolicyGroup readFromZK(long version, MetaToolOptions options) throws KeeperException {
        try {
            String          vPath;
            
            vPath = getVersionPath(version);
            return new PolicyParser().parsePolicyGroup(zk.getString(vPath), version);
        } catch (PolicyParseException ppe) {
            throw new RuntimeException(ppe);
        }
    }

    @Override
    public void writeToFile(File file, StoragePolicyGroup policyGroup) throws IOException {
        BufferedWriter  writer;
        
        writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
        writer.write(policyGroup.getPolicy(policyGroup.getName()).toString());
        for (StoragePolicy policy : policyGroup.getPolicies()) {
            if (!policy.getName().equals(policyGroup.getName())) {
                writer.write(policy.toString());
            }
        }
        writer.close();
    }

    @Override
    public String writeToZK(StoragePolicyGroup policyGroup, MetaToolOptions options) throws IOException, KeeperException {
        zk.createString(base +"/" , policyGroup.toString(), CreateMode.PERSISTENT_SEQUENTIAL);
        return null;
    }
}

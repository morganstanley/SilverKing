package com.ms.silverking.cloud.management;

import java.io.File;
import java.io.IOException;

import org.apache.zookeeper.KeeperException;

public interface MetaToolModule<T> {
    public T readFromFile(File file, long version) throws IOException;
    public T readFromZK(long version, MetaToolOptions options) throws KeeperException;
    public void writeToFile(File file, T instance) throws IOException;
    /** 
     * Write to zookeeper using the version provided and ignoring any
     * version already present.
     * @param options TODO
     * @return TODO
     */
    public String writeToZK(T instance, MetaToolOptions options) throws IOException, KeeperException;
    public void deleteFromZK(long version) throws KeeperException;
    public long getLatestVersion() throws KeeperException;
}

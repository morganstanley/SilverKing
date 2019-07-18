package com.ms.silverking.cloud.dht.daemon.storage;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;

import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.common.CCSSUtil;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;
import com.ms.silverking.thread.lwt.LWTThreadUtil;

public abstract class ReadThroughPlugin {

    private static final String implProp = ReadThroughPlugin.class.getPackage().getName() + ".ReadThroughImpl";

    static {
        ObjectDefParser2.addParserWithExclusions(ReadThroughPlugin.class, null, FieldsRequirement.ALLOW_INCOMPLETE, null);
    }

    static ReadThroughPlugin getPlugin() {
        String property;

        property = System.getProperty(implProp);
        if (property == null) {
            return new NoopReadThroughPlugin();
        } else {
            return ObjectDefParser2.parse(property, ReadThroughPlugin.class.getPackage());
        }
    }

    private static byte[] checksum = new byte[8];

    static {
        Arrays.fill(checksum, (byte) 0);
    }

    private StorageParametersAndRequirements getStorageParameters(ByteBuffer result) {
        StorageParametersAndRequirements paramsAndReqs;
        short ccss;


        ccss = CCSSUtil.createCCSS(Compression.NONE, ChecksumType.NONE);

        paramsAndReqs = new StorageParametersAndRequirements(
                1, // Only single version supported!
                result.capacity(),
                -1,
                ccss,
                checksum,
                new byte[]{1, 1},
                Instant.now().toEpochMilli(),
                PutOptions.noVersionRequired);

        return paramsAndReqs;
    }

    private void putBatch(DHTKey[] keys, ByteBuffer[] items, NamespaceStore ns) {
        for (int i = 0; i < keys.length; i++) {
            DHTKey k = keys[i];
            ByteBuffer v = items[i];
            putSingle(k, v, ns);
        }
    }

    private void putSingle(DHTKey key, ByteBuffer item, NamespaceStore ns) {
        if (item != null) {
            StorageParametersAndRequirements params = getStorageParameters(item);
            OpResult res = ns._put(key, item, params, new byte[]{}, NamespaceVersionMode.SINGLE_VERSION);
            notifyPutComplete(key, item, res);
        }
    }

    final ByteBuffer[] readThroughBatch(DHTKey[] keys, InternalRetrievalOptions options, NamespaceStore ns) {
        LWTThreadUtil.setBlocked();
        notifyReadThroughBatch(keys, options.getUserOptions());
        ByteBuffer[] results = readThroughBatchImpl(keys, options.getUserOptions());

        notifyPutBatch(keys, results);
        putBatch(keys, results, ns);

        LWTThreadUtil.setNonBlocked();
        return results;
    }

    final ByteBuffer readThroughSingle(DHTKey key, InternalRetrievalOptions options, NamespaceStore ns) {
        LWTThreadUtil.setBlocked();
        notifyReadThrough(key, options.getUserOptions());
        ByteBuffer result = readThroughSingleImpl(key, options.getUserOptions());

        notifyPutSingle(key, result);
        putSingle(key, result, ns);

        LWTThreadUtil.setNonBlocked();
        return result;
    }

    abstract String getName();

    // Required implementation hooks for plugins
    protected abstract ByteBuffer[] readThroughBatchImpl(DHTKey[] keys, byte[] userOptions);

    protected abstract ByteBuffer readThroughSingleImpl(DHTKey key, byte[] userOptions);

    // Optional hooks for e.g. logging, metrics
    protected void notifyReadThroughBatch(DHTKey[] keys, byte[] userOptions) { }

    protected void notifyReadThrough(DHTKey key, byte[] userOptions) { }

    protected void notifyPutBatch(DHTKey[] keys, ByteBuffer[] items) { }

    protected void notifyPutSingle(DHTKey key, ByteBuffer item) { }

    protected void notifyPutComplete(DHTKey key, ByteBuffer item, OpResult res) { }

}


package com.ms.silverking.cloud.dht.meta;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.log.Log;
import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.thread.ThreadUtil;

public class DHTRingCurTargetZK {
    private final MetaClient        mc;
    private final DHTConfiguration  dhtConfig;
    
	public enum NodeType {Current, Target, Master};
    
    private static final int    retries = 4;
    private static final int    retrySleepMS = 2 * 1000;
    static final int			minValueLength = NumConversion.BYTES_PER_LONG * 2;
    
    private static final boolean	debug = true;
    
    public DHTRingCurTargetZK(MetaClient mc, DHTConfiguration dhtConfig) throws KeeperException {
        this.mc = mc;
        this.dhtConfig = dhtConfig;
        ensureBasePathExists();
        ensureDataNodesExist();
    }
    
    private void ensureBasePathExists() throws KeeperException {
        mc.ensureMetaPathsExist();
    }
    
    private void ensureDataNodesExist() throws KeeperException {
        mc.getZooKeeper().ensureCreated(mc.getMetaPaths().getInstanceCurRingAndVersionPairPath(mc.getDHTName()));
        mc.getZooKeeper().ensureCreated(mc.getMetaPaths().getInstanceTargetRingAndVersionPairPath(mc.getDHTName()));
    }
    
    public void setRingAndVersionPair(NodeType nodeType, String ringName, Pair<Long,Long> version) throws KeeperException {
    	switch (nodeType) {
    	case Current: setCurRingAndVersionPair(ringName, version); break;
    	case Target: setTargetRingAndVersionPair(ringName, version); break;
    	case Master: setMasterRingAndVersionPair(ringName, version); break;
    	default: throw new RuntimeException("Panic");
    	}
    }
    
    public Triple<String,Long,Long> getRingAndVersionPair(NodeType nodeType) throws KeeperException {
    	switch (nodeType) {
    	case Current: return getCurRingAndVersionPair();
    	case Target: return getTargetRingAndVersionPair();
    	case Master: return getMasterRingAndVersionPair();
    	default: throw new RuntimeException("Panic");
    	}
    }

    public void setCurRingAndVersionPair(String ringName, Pair<Long,Long> version) throws KeeperException {
    	setCurRingAndVersionPair(ringName, version.getV1(), version.getV2());
    }
    
    public void setCurRingAndVersionPair(String ringName, long ringConfigVersion, long configInstanceVersion) throws KeeperException {
        setRingAndVersionPair(ringName, ringConfigVersion,configInstanceVersion, mc.getMetaPaths().getInstanceCurRingAndVersionPairPath(mc.getDHTName()));
    }
    
    public Triple<String,Long,Long> getCurRingAndVersionPair(Stat stat) throws KeeperException {
        return getRingAndVersion(mc.getMetaPaths().getInstanceCurRingAndVersionPairPath(mc.getDHTName()), stat);
    }
    
    public Triple<String,Long,Long> getCurRingAndVersionPair() throws KeeperException {
        return getRingAndVersion(mc.getMetaPaths().getInstanceCurRingAndVersionPairPath(mc.getDHTName()));
    }

    public void setTargetRingAndVersionPair(String ringName, Pair<Long,Long> version) throws KeeperException {
    	setTargetRingAndVersionPair(ringName, version.getV1(), version.getV2());
    }
    
    public void setTargetRingAndVersionPair(String ringName, long ringConfigVersion, long configInstanceVersion) throws KeeperException {
        setRingAndVersionPair(ringName, ringConfigVersion, configInstanceVersion, mc.getMetaPaths().getInstanceTargetRingAndVersionPairPath(mc.getDHTName()));
    }
    
    public Triple<String,Long,Long> getTargetRingAndVersionPair() throws KeeperException {
        return getRingAndVersion(mc.getMetaPaths().getInstanceTargetRingAndVersionPairPath(mc.getDHTName()));
    }

    public void setMasterRingAndVersionPair(String ringName, Pair<Long,Long> version) throws KeeperException {
    	setMasterRingAndVersionPair(ringName, version.getV1(), version.getV2());
    }
    
    public void setMasterRingAndVersionPair(String ringName, long ringConfigVersion, long configInstanceVersion) throws KeeperException {
        setRingAndVersionPair(ringName, ringConfigVersion, configInstanceVersion, mc.getMetaPaths().getInstanceMasterRingAndVersionPairPath(mc.getDHTName()));
    }
    
    public Triple<String,Long,Long> getMasterRingAndVersionPair() throws KeeperException {
        return getRingAndVersion(mc.getMetaPaths().getInstanceMasterRingAndVersionPairPath(mc.getDHTName()));
    }
    
    
    private void setRingAndVersionPair(String ringName, long ringConfigVersion, long configInstanceVersion, String path) throws KeeperException {
        int     attemptIndex;
        boolean complete;
        
        attemptIndex = 0;
        complete = false;
        while (!complete) {
            try {
            	Stat	stat;

            	if (!mc.getZooKeeper().exists(path)) {
            		mc.getZooKeeper().create(path);
            	}
                stat = mc.getZooKeeper().set(path, nameAndVersionToBytes(ringName, ringConfigVersion, configInstanceVersion));
                if (debug) {
                	Log.warning(path);
                    Log.warning(stat);
                }
                complete = true;
            } catch (KeeperException ke) {
                Log.logErrorWarning(ke, "Exception in DHTRingCurTargetZK.setRingAndVersion(). Attempt: "+ attemptIndex);
                if (attemptIndex >= retries) {
                    throw ke;
                } else {
                    ++attemptIndex;
                    ThreadUtil.randomSleep(retrySleepMS, retrySleepMS << attemptIndex);
                }
            }
        }
    }
    
    private Triple<String,Long,Long> getRingAndVersion(String path) throws KeeperException {
    	return getRingAndVersion(path, null);
    }
    
    private Triple<String,Long,Long> getRingAndVersion(String path, Stat stat) throws KeeperException {
    	if (!mc.getZooKeeper().exists(path)) {
    		return null;
    	} else {
        	byte[]	b;
        	
	    	b = mc.getZooKeeper().getByteArray(path, null, stat);
	    	if (b.length < minValueLength) {
	    		return null;
	    	} else {
	    		return bytesToNameAndVersion(b);
	    	}
    	}
    }
    
    public static byte[] nameAndVersionToBytes(String ringName, long ringConfigVersion, long configInstanceVersion) {
        byte[]  nb;
        byte[]  b;

        nb = ringName.getBytes();
        b = new byte[nb.length + NumConversion.BYTES_PER_LONG * 2];
        System.arraycopy(nb, 0, b, 0, nb.length);
        NumConversion.longToBytes(ringConfigVersion, b, nb.length);
        NumConversion.longToBytes(configInstanceVersion, b, nb.length + NumConversion.BYTES_PER_LONG);
        return b;
    }
    
    public static Triple<String,Long,Long> bytesToNameAndVersion(byte[]  b) {
        if (b.length < minValueLength) {
            throw new RuntimeException("b.length too small: "+ b.length);
        } else {
            byte[]  nb;
            long    v2;
            long    v3;
            
            nb = new byte[b.length - NumConversion.BYTES_PER_LONG * 2];
            System.arraycopy(b, 0, nb, 0, nb.length);
            v2 = NumConversion.bytesToLong(b, nb.length);
            v3 = NumConversion.bytesToLong(b, nb.length + NumConversion.BYTES_PER_LONG);
            return Triple.of(new String(nb), v2, v3);
        }
    }
    
    public boolean curAndTargetRingsMatch() throws KeeperException {
    	return getCurRingAndVersionPair().equals(getTargetRingAndVersionPair());
    }
}

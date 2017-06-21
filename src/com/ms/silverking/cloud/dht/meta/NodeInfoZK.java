package com.ms.silverking.cloud.dht.meta;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.ms.silverking.cloud.dht.daemon.DHTNodeConfiguration;
import com.ms.silverking.cloud.dht.daemon.NodeInfo;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.os.linux.fs.DF;

public class NodeInfoZK implements Watcher {
    private final MetaClient    mc;
    private final IPAndPort     myIPAndPort;
    private final String        instanceNodeInfoPath;
    private volatile NodeInfo	nodeInfo;
    
    private static final int 	infoCheckPeriodMillis = 1 * 60 * 1000;
    private static final int	dfTimeoutSeconds = 2 * 60;
    private static final int	nodeInfoCheckPeriodMillis = 100; 
    
    private static final boolean    verbose = true;
    
    public NodeInfoZK(MetaClient mc, IPAndPort myIPAndPort, Timer timer) {
        this.mc = mc;
        this.myIPAndPort = myIPAndPort;
        instanceNodeInfoPath = mc.getMetaPaths().getInstanceNodeInfoPath();
        timer.scheduleAtFixedRate(new InfoChecker(), 
                ThreadLocalRandom.current().nextInt(infoCheckPeriodMillis), 
                infoCheckPeriodMillis);
    }
    
    private String getNodeInfoPath(IPAndPort node) {
        return instanceNodeInfoPath +"/"+ node;
    }
    
    public void setNodeInfo(NodeInfo nodeInfo) {
    	this.nodeInfo = nodeInfo;
    	ensureNodeInfoSet();
    }
    
    public void ensureNodeInfoSet() {
        try {
        	if (nodeInfo != null) {
	        	mc.getZooKeeper().setEphemeral(getNodeInfoPath(myIPAndPort), nodeInfo.toArray());
        	}
        } catch (KeeperException ke) {
            Log.logErrorWarning(ke);
        }
    }
    
    @Override
    public void process(WatchedEvent event) {
    	Log.fine(event);
        //if (mc.getZooKeeper().getState() == States.CONNECTED) {
        //    ensureStateSet();
        //}
    	switch (event.getType()) {
    	case None:
    		if (event.getState() == KeeperState.SyncConnected) {
    			ensureNodeInfoSet();
    		}
    		break;
    	case NodeCreated:
    		break;
    	case NodeDeleted:
    		break;
    	case NodeDataChanged:
    		break;
    	case NodeChildrenChanged:
    		break;
    	default:
    		Log.warning("Unknown event type: ", event.getType());
    	}
    }
    
    public NodeInfo getNodeInfo(IPAndPort node) throws KeeperException {
    	try {
	    	byte[]	data;
	    	
	    	data = mc.getZooKeeper().getByteArray(getNodeInfoPath(node), this);
            return NodeInfo.fromArray(data);
    	} catch (NoNodeException nne) {
    		return null;
    	}
    }
    
    public Map<IPAndPort, NodeInfo> getNodeInfo(Set<IPAndPort> nodes) throws KeeperException {
        Map<IPAndPort, NodeInfo> allNodeInfo;
        
        allNodeInfo = new HashMap<>();
        for (IPAndPort node : nodes) {
            NodeInfo	nodeInfo;
            
            nodeInfo = getNodeInfo(node);
    		allNodeInfo.put(node, nodeInfo);
        }
        return allNodeInfo;
    }
    
    class InfoChecker extends TimerTask {
        InfoChecker() {
        }
        
        @Override
        public void run() {
            setNodeInfo(getNodeInfo());
        }
        
        public NodeInfo getNodeInfo() {
        	Triple<Long,Long,Integer>	dfInfo;
        	
        	try {
				dfInfo = DF.df(DHTNodeConfiguration.dataBasePath, dfTimeoutSeconds);
				return new NodeInfo(dfInfo.getV1(), dfInfo.getV2(), dfInfo.getV3());
			} catch (Exception e) {
				Log.logErrorWarning(e, "Unable to getNodeInfo(");
				return null;
			}
        }
    }
}

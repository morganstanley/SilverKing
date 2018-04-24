package com.ms.silverking.cloud.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapMaker;
import com.ms.silverking.collection.LightLinkedBlockingQueue;
import com.ms.silverking.log.Log;
import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.process.SafeThread;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

/**
 * Extension of the raw ZooKeeper class used to simplify some interaction.
 */
public class ZooKeeperExtended extends ZooKeeper implements AsyncCallback.StringCallback, AsyncCallback.VoidCallback,
        AsyncCallback.ChildrenCallback, AsyncCallback.DataCallback, AsyncCallback.StatCallback, AsyncCallback.Children2Callback {
    private final ZooKeeperConfig zkConfig;
//    private final ConcurrentMap<ZKRequestUUID, ActiveOp> activeOps;
    private final ConcurrentMap<ZKRequestUUID, AsyncCallbackOp>   callbacks;
    
    private static final LightLinkedBlockingQueue<Result>	asyncGetResults;
    private static final int	processRunnerIdleTimeoutSeconds = 10;
    private static final int	processRunnerThreads = 6;
    
    private static final int    connectionCheckIntervalMillis = 100;
    private static final List<ACL> defaultACL = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    
    private static final int	displayMissingIntervalSeconds = 20;
    private static final int	reissueIntervalSeconds = 1 * 60;
    private static final int	timeoutSeconds = 3 * 1 * 60 + 10;
    
    private static final int ANY_VERSION = -1;
    
    public static final int AUTO_VERSION_FIELD_SIZE = 10;
    
    private final Watcher   watcher;
    
    static {
        asyncGetResults = new LightLinkedBlockingQueue<>();
    	new ProcessRunner();
    }
    
    /**
     * @param host
     * @param sessionTimeout
     * @param watcher
     * @throws KeeperException
     * @throws IOException
     */
    public ZooKeeperExtended(ZooKeeperConfig zkConfig, int sessionTimeout, Watcher watcher) throws KeeperException,
            IOException {
        super(zkConfig.getConnectString(), sessionTimeout, watcher);
        this.zkConfig = zkConfig;
        this.watcher = watcher;
        //activeOps = new ConcurrentHashMap<ZKRequestUUID, ActiveOp>();
        callbacks = new MapMaker().weakValues().makeMap();
    }

    /**
     * @param host
     * @param sessionTimeout
     * @param watcher
     * @param sessionId
     * @param sessionPasswd
     * @throws KeeperException
     * @throws IOException
     */
    public ZooKeeperExtended(ZooKeeperConfig zkConfig, int sessionTimeout, Watcher watcher, long sessionId,
            byte[] sessionPasswd) throws KeeperException, IOException {
        super(zkConfig.getConnectString(), sessionTimeout, watcher, sessionId, sessionPasswd);
        this.zkConfig = zkConfig;
        this.watcher = watcher;
        //activeOps = new ConcurrentHashMap<ZKRequestUUID, ActiveOp>();
        callbacks = new MapMaker().weakValues().makeMap();
    }
    
    public ZooKeeperConfig getZKConfig() {
        return zkConfig;
    }
    
    public Watcher getWatcher() {
        return watcher;
    }
    
    ////////////////////////
    // callback processing

    // VoidCallback
    @Override
    public void processResult(int rc, String path, Object ctx) {
        VoidCallbackOp    voidCallbackOp;

        voidCallbackOp = (VoidCallbackOp)callbacks.get((ZKRequestUUID)ctx);
        if (voidCallbackOp != null) {
            voidCallbackOp.setComplete(rc, path);
        } else {
            Log.warning("No callback for: ", ctx);
        }
    }

    // DataCallback
    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
        DataCallbackOp    callbackOp;

        callbackOp = (DataCallbackOp)callbacks.get((ZKRequestUUID)ctx);
        if (callbackOp != null) {
            callbackOp.setComplete(rc, path, data, stat);
        } else {
            Log.warning("No callback for: ", ctx);
        }
    }

    // StatCallback
    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
        StatCallbackOp    callbackOp;

        callbackOp = (StatCallbackOp)callbacks.get((ZKRequestUUID)ctx);
        if (callbackOp != null) {
            callbackOp.setComplete(rc, path, stat);
        } else {
            Log.warning("No callback for: ", ctx);
        }
    }

    // StringCallback
    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
        StringCallbackOp    callbackOp;

        callbackOp = (StringCallbackOp)callbacks.get((ZKRequestUUID)ctx);
        if (callbackOp != null) {
            callbackOp.setComplete(rc, path, name);
        } else {
            Log.warning("No callback for: ", ctx);
        }
    }
    
    // Children2Callback
    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
        Children2CallbackOp    callbackOp;

        callbackOp = (Children2CallbackOp)callbacks.get((ZKRequestUUID)ctx);
        if (callbackOp != null) {
            callbackOp.setComplete(rc, path, children, stat);
        } else {
            Log.warning("No callback for: ", ctx);
        }
    }

    // ChildrenCallback
    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children) {
        ChildrenCallbackOp    callbackOp;

        callbackOp = (ChildrenCallbackOp)callbacks.get((ZKRequestUUID)ctx);
        if (callbackOp != null) {
            callbackOp.setComplete(rc, path, children);
        } else {
            Log.warning("No callback for: ", ctx);
        }
    }
    
    //////////////////////
    // async op creation
    
    private void addAsyncCallbackOp(AsyncCallbackOp op) {
        callbacks.put(op.getRequestUUID(), op);
    }
    
    private ACLCallbackOp newACLCallbackOp() {
        ACLCallbackOp    op;
        
        op = new ACLCallbackOp();
        addAsyncCallbackOp(op);
        return op;
    }
    
    private Children2CallbackOp newChildren2CallbackOp() {
        Children2CallbackOp    op;
        
        op = new Children2CallbackOp();
        addAsyncCallbackOp(op);
        return op;
    }
    
    private ChildrenCallbackOp newChildrenCallbackOp() {
        ChildrenCallbackOp    op;
        
        op = new ChildrenCallbackOp();
        addAsyncCallbackOp(op);
        return op;
    }
    
    private DataCallbackOp newDataCallbackOp() {
        DataCallbackOp    op;
        
        op = new DataCallbackOp();
        addAsyncCallbackOp(op);
        return op;
    }
    
    private StatCallbackOp newStatCallbackOp() {
        StatCallbackOp    op;
        
        op = new StatCallbackOp();
        addAsyncCallbackOp(op);
        return op;
    }
    
    private VoidCallbackOp newVoidCallbackOp() {
        VoidCallbackOp    op;
        
        op = new VoidCallbackOp();
        addAsyncCallbackOp(op);
        return op;
    }
    
    private StringCallbackOp newStringCallbackOp() {
        StringCallbackOp    op;
        
        op = new StringCallbackOp();
        addAsyncCallbackOp(op);
        return op;
    }
    
    ///////////////
    // operations
    
    public void setEphemeralInteger(String path, int data) throws KeeperException {
        setEphemeral(path, NumConversion.intToBytes(data));
    }
    
    public void setEphemeral(String path, byte[] data) throws KeeperException {
        if (exists(path)) {
            set(path, data);
        } else {
            create(path, data, CreateMode.EPHEMERAL);
        }
    }

    public Stat set(String path, byte[] data, int version) throws KeeperException {
        try {
            return setData(path, data, version);
        } catch (InterruptedException ie) {
            throw new RuntimeException("panic");
        }
    }

    /**
     * set for any version
     * @param path
     * @param data
     * @return
     * @throws KeeperException
     */
    public Stat set(String path, byte[] data) throws KeeperException {
        return set(path, data, -1);
    }
    
    public Stat setString(String path, String data) throws KeeperException {
        return set(path, data.getBytes());
    }
    
    public Stat setInteger(String path, int x) throws KeeperException {
        return set(path, NumConversion.intToBytes(x));
    }
    
    public String create(String path, byte[] data, CreateMode createMode) throws KeeperException {
        try {
            return create(path, data, defaultACL, createMode);
        } catch (InterruptedException ie) {
            throw new RuntimeException("panic");
        }
    }
    
    public String create(String path, byte[] data) throws KeeperException {
        return create(path, data, CreateMode.PERSISTENT);
    }
    
    public String create(String path) throws KeeperException {
        return create(path, "".getBytes());
    }
    
    public String ensureCreated(String path) throws KeeperException {
    	if (!exists(path)) {
    		return create(path, "".getBytes());
    	} else {
    		return path;
    	}
    }
    
    public StringCallbackOp createAsync(String path, byte[] data, List<ACL> acl, CreateMode createMode) 
                                throws KeeperException {
        StringCallbackOp    op;
        
        op = newStringCallbackOp();
        create(path, data, acl, createMode, this, op);
        return op;
    }
        
    public StringCallbackOp createAsync(String path, byte[] data) 
            throws KeeperException {
        return createAsync(path, data, defaultACL, CreateMode.PERSISTENT);
    }
    
    public void delete(String path) throws KeeperException {
        try {
            delete(path, -1);
        } catch (InterruptedException ie) {
            throw new RuntimeException("panic");
        }
    }
    
    public void waitForCompletion(List<? extends AsyncCallbackOp> ops) throws KeeperException {
        for (AsyncCallbackOp op : ops) {
            op.waitForCompletion();
        }
    }
    
    public VoidCallbackOp deleteAsync(String path) {
        VoidCallbackOp  op;
        
        op = newVoidCallbackOp();
        delete(path, ANY_VERSION, this, op.getRequestUUID());
        return op;
    }

    public void deleteRecursive(String path) throws KeeperException {
        deleteChildrenRecursive(path);
        delete(path);
    }
    
    private void deleteChildrenRecursive(String path) throws KeeperException {
        List<String>    children;
        List<VoidCallbackOp>   ops;
        
        children = getChildren(path);
        for (String child : children) {
            deleteChildrenRecursive(path +"/"+ child);
        }
        ops = new ArrayList<>(children.size());
        for (String child : children) {
            ops.add(deleteAsync(path +"/"+ child));
        }
        waitForCompletion(ops);
    }

    public void createInt(String path, int val) throws KeeperException {
        create(path, NumConversion.intToBytes(val));
    }

    public int getInt(String path) throws KeeperException {
    	return getInt(path, null);
    }

    public int getInt(String path, Watcher watcher) throws KeeperException {
        try {
            byte[] data;
            Stat stat;

            stat = new Stat();
            data = getData(path, watcher, stat);
            return NumConversion.bytesToInt(data);
        } catch (InterruptedException ie) {
            throw new RuntimeException("panic");
        }
    }
    
    public void createDouble(String path, double val) throws KeeperException {
        create(path, NumConversion.doubleToBytes(val));
    }

    public double getDouble(String path) throws KeeperException {
        try {
            byte[] data;
            Stat stat;

            stat = new Stat();
            data = getData(path, false, stat);
            return NumConversion.bytesToDouble(data);
        } catch (InterruptedException ie) {
            throw new RuntimeException("panic");
        }
    }
    
    public StringCallbackOp createStringAsync(String path, String val) throws KeeperException {
        return createAsync(path, val.getBytes());
    }

    public String createString(String path, String val, CreateMode createMode) throws KeeperException {
        return create(path, val.getBytes(), createMode);
    }
    
    public void createString(String path, String val) throws KeeperException {
        create(path, val.getBytes());
    }
    
    public void createInteger(String path, int v) throws KeeperException {
        create(path, NumConversion.intToBytes(v));
    }
    
    public Map<String,String> getStrings(Set<String> paths) throws KeeperException {
    	return convertToStringData(getByteArrays(paths));
    }
    
    public Map<String,String> getStrings(Set<String> paths, Watcher watcher) throws KeeperException {
    	return convertToStringData(getByteArrays(paths, watcher));
    }
    
	public Map<String, Integer> getInts(Set<String> paths) throws KeeperException {
    	return convertToIntData(getByteArrays(paths));
	}    
    
	public Map<String, Integer> getInts(Set<String> paths, Watcher watcher) throws KeeperException {
    	return convertToIntData(getByteArrays(paths, watcher));
	}
	
	public Map<String,byte[]> getByteArrays(Set<String> paths) throws KeeperException {
    	AsyncGet	aGet;
    	
    	aGet = new AsyncGet(paths);
    	aGet.issueGets();
    	aGet.waitForCompletion();
    	return aGet.getDataMap();
    }
    
	public Map<String,byte[]> getByteArrays(Set<String> paths, Watcher watcher) throws KeeperException {
    	AsyncGet	aGet;
    	
    	aGet = new AsyncGet(paths, watcher, null);
    	aGet.issueGets();
    	aGet.waitForCompletion();
    	return aGet.getDataMap();
    }
	
    public Map<String,byte[]> getByteArrays(String basePath, Set<String> children, Watcher watcher, CancelableObserver observer) throws KeeperException {
        ImmutableSet.Builder<String>	childrenPaths;
    	AsyncGet	aGet;
        
        childrenPaths = ImmutableSet.builder();
        for (String child : children) {
        	childrenPaths.add(basePath +"/"+ child);
        }
    	
    	aGet = new AsyncGet(childrenPaths.build(), watcher, observer);
    	aGet.issueGets();
    	aGet.waitForCompletion();
    	
    	return stripBasePathFromKeys(aGet.getDataMap(), basePath);
    }
    
    public Map<String,byte[]> getByteArrays(String basePath, Set<String> children) throws KeeperException {
    	return getByteArrays(basePath, children, null, null);
    }
    
    private Map<String, byte[]> stripBasePathFromKeys(Map<String, byte[]> rawMap, String basePath) {
		ImmutableMap.Builder<String, byte[]>	strippedMap;
		
		strippedMap = ImmutableMap.builder();
		for (Map.Entry<String, byte[]> entry : rawMap.entrySet()) {
			strippedMap.put(stripBasePath(entry.getKey(), basePath), entry.getValue());
		}
		return strippedMap.build();
	}
    
    private String stripBasePath(String rawPath, String basePath) {
    	if (rawPath.startsWith(basePath +"/")) {
    		return rawPath.substring(basePath.length() + 1);
    	} else {
    		throw new RuntimeException(String.format("%s does not start with %s", rawPath, basePath));
    	}
    }
    
	//////////////////////////////////////////
    
	class AsyncGet implements DataCallback {
		private final CancelableObserver observer;
    	private final Set<String>	paths;
    	private final Map<String,byte[]>	dataMap;
    	private final Lock	lock;
    	private final Condition	cv;
    	private volatile boolean	complete;
    	private Watcher	watcher;
    	private int	lastNumIncomplete;
    	private Stopwatch	retrySW;
    	private Stopwatch	timeoutSW;
    	private Set<String>	errorKeys;
    	private Stopwatch	displayMissingSW;
    	
    	AsyncGet(Set<String> paths, Watcher	watcher, CancelableObserver observer) {
    		this.paths = paths;
    		this.dataMap = new ConcurrentHashMap<>();
    		this.errorKeys = new ConcurrentSkipListSet<>();
    		this.lock = new ReentrantLock();
    		this.cv = lock.newCondition();
    		this.watcher = watcher;
    		this.observer = observer;
    		retrySW = new SimpleStopwatch();
    		timeoutSW = new SimpleStopwatch();
    		displayMissingSW = new SimpleStopwatch();
    	}
    	
    	AsyncGet(Set<String> paths) {
    		this(paths, null, null);
    	}
    	
		Map<String,byte[]> getDataMap() {
    		return dataMap;
    	}
    	
    	void issueGets() {
    		for (String path : paths) {
    			//System.out.println("g: "+ path);
    			getData(path, watcher, this, null);
    		}
    		retrySW.reset();
    	}
    	
		public void issueGetsForIncomplete() {
			if (observer == null || observer.isActive()) {
				for (String path : paths) {
					if (!dataMap.containsKey(path) && !errorKeys.contains(path)) {
						Log.warning("Issuing sync zk get: ", path);
		    			try {
							byte[]	data;
							
							data = getData(path, watcher, null);
							if (data != null) {
								Log.warning("Sync zk get success: ", path);
								dataMap.put(path, data);
							} else {
								Log.warning("Sync zk get failed to retrieve data; adding to error keys: ", path);
								errorKeys.add(path);
							}
						} catch (KeeperException | InterruptedException e) {
							Log.logErrorWarning(e, "Sync zk get hit exception");
						}
						//Log.warning("Re-issuing zk get: ", path);
		    			//getData(path, watcher, this, null);
					}
				}
	    		retrySW.reset();
			}
		}
    	
		public void markIncompleteAsErrors() {
			for (String path : paths) {
				if (!dataMap.containsKey(path)) {
					Log.warning("Timed out: ", path);
					errorKeys.add(path);
				}
			}
    		retrySW.reset();
			notifyWaiter();
		}
    	
		// precondition: only a single caller allowed in this method
    	public void waitForCompletion() throws KeeperException {
			while (!complete && (observer == null || observer.isActive())) {
				boolean	displayMissing;
				
				displayMissing = false;
				try {
		    		lock.lock();
		    		try {
						cv.await(displayMissingIntervalSeconds, TimeUnit.SECONDS);
		    		} finally {
		    			lock.unlock();
		    		}
					if (displayMissingSW.getSplitSeconds() > displayMissingIntervalSeconds) {
						displayMissing = true;
						displayMissingSW.reset();
					}
				} catch (InterruptedException ie) {
				}
				if (!complete) {
					if (getState() != ZooKeeper.States.CONNECTED) {
						throw KeeperException.create(Code.SESSIONEXPIRED);
					}
				}
				if (!complete) {
					checkForCompletion(displayMissing);
				}
			}
		}

		private void checkForCompletion(boolean displayMissing) {
			if (observer == null || observer.isActive()) {
				int		numIncomplete;
				
				numIncomplete = 0;
				for (String path : paths) {
					if (!dataMap.containsKey(path) && !errorKeys.contains(path)) {
						++numIncomplete;
						if (displayMissing) {
							Log.warning("checkForCompletion still waiting for: ", path);
						}
						break;
					}
				}
	    		if (numIncomplete == 0) {
	    			complete = true;
	    		} else {
	    			if (retrySW.getSplitSeconds() > reissueIntervalSeconds) {
	    				if (timeoutSW.getSplitSeconds() > timeoutSeconds) {
	    					markIncompleteAsErrors();
	    				} else {
	    					issueGetsForIncomplete();
	    				}
	    			}
	    		}
			}
		}
		
		/**
		 * Notify waiter. Let the waiter check for completion, resend etc.
		 * We want to keep callback operations as light as possible.
		 */
		private void notifyWaiter() {
			lock.lock();
			try {
				cv.signalAll();
			} finally {
				lock.unlock();
			}
			
		}
		
		@Override
		public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
			try {
				asyncGetResults.put(new Result(this, rc, path, ctx, data, stat));
			} catch (InterruptedException e) {
				Log.logErrorWarning(e);
			}
		}
		
		public void processSafely(int rc, String path, Object ctx, byte[] data, Stat stat) {
			try {
				if (rc == 0 && data != null) {
					//System.out.printf("rc %d path %s\n", rc, path);
					dataMap.put(path, data);
				} else {
					Log.warningf("Error %d reading path %s", rc, path);
					errorKeys.add(path);
				}
				notifyWaiter();
				//System.out.println(complete);
			} catch (RuntimeException re) {
				re.printStackTrace();
			//} finally {
				//System.out.println("!"+ complete);
			}
		}
    }
	
    static class ProcessRunner implements Runnable {
    	ProcessRunner() {
    		for (int i = 0; i < processRunnerThreads; i++) {
    			new SafeThread(this, "ZKE.ProcessRunner."+ i, true).start();
    		}
    	}
    	
    	public void run() {
    		while (true) {
	    		try {
	    			Result	r;
	    			
	    			r = asyncGetResults.poll(processRunnerIdleTimeoutSeconds, TimeUnit.SECONDS);
	    			if (r != null) {
	    				r.asyncGet.processSafely(r.rc, r.path, r.ctx, r.data, r.stat);
	    			} else {
	    				// FUTURE - have a pool of threads that can resize when some time out
	    				// or consider using lwt
	    			}
	    		} catch (Exception e) {
	    			e.printStackTrace();
	    		}
    		}
    	}
    }
    
	class Result {
		private final AsyncGet	asyncGet;
		private final int		rc;
		private final String	path;
		private final Object	ctx;
		private final byte[]	data;
		private final Stat		stat;
		
		Result(AsyncGet asyncGet, int rc, String path, Object ctx, byte[] data, Stat stat) {
			this.asyncGet = asyncGet;
			this.rc = rc;
			this.path = path;
			this.ctx = ctx;
			this.data = data;
			this.stat = stat;
		}
	}		

	//////////////////////////////////////////
    
    private Map<String,String> convertToStringData(Map<String,byte[]> data) {
    	Map<String,String>	stringData;
    	
    	stringData = new HashMap<>();
    	for (Map.Entry<String, byte[]> e : data.entrySet()) {
    		stringData.put(e.getKey(), new String(e.getValue()));
    	}
    	return stringData;
    }
    
    private Map<String, Integer> convertToIntData(Map<String, byte[]> data) {
    	Map<String,Integer>	intData;
    	
    	intData = new HashMap<>();
    	for (Map.Entry<String, byte[]> e : data.entrySet()) {
    		intData.put(e.getKey(), NumConversion.bytesToInt(e.getValue()));
    	}
    	return intData;
	}
        
    public byte[] getByteArray(String path) throws KeeperException {
    	return getByteArray(path, null);
    }

    public byte[] getByteArray(String path, Watcher watcher) throws KeeperException {
    	return getByteArray(path, watcher, new Stat());
    }
    
    public byte[] getByteArray(String path, Watcher watcher, Stat stat) throws KeeperException {
        try {
            byte[]  data;

            data = getData(path, watcher, stat);
            return data;
        } catch (InterruptedException ie) {
            throw new RuntimeException("panic");
        }
    }
    
    public String getString(String path) throws KeeperException {
        byte[]  data;

        data = getByteArray(path);
        return new String(data);
    }

    public String getString(String path, Watcher watcher, Stat stat) throws KeeperException {
        byte[]  data;

        data = getByteArray(path, watcher, stat);
        return new String(data);
    }
    
    public int getInteger(String path) throws KeeperException {
        byte[]  data;

        data = getByteArray(path);
        return NumConversion.bytesToInt(data);
    }
    
    public boolean exists(String path) throws KeeperException {
        try {
            return super.exists(path, false) != null;
        } catch (InterruptedException ie) {
            throw new RuntimeException("panic");
        }
    }

    public List<String> getChildren(String path, Watcher watcher) throws KeeperException {
        try {
            return super.getChildren(path, watcher);
        } catch (InterruptedException ie) {
            throw new RuntimeException("panic");
        }
    }

    public List<String> getChildren(String path) throws KeeperException {
        try {
        	return getChildren(path, false);
        } catch (InterruptedException ie) {
            throw new RuntimeException("panic");
        }
    }
    
    public List<String> getChildrenPriorTo(String path, long zxid) throws KeeperException {
        List<String>    allChildren;
        List<String>    priorChildren;
        
        allChildren = getChildren(path);
        priorChildren = new ArrayList<>();
        for (String child : allChildren) {
            //System.out.println("\t"+ child);
            //System.out.println("\t\t"+ getStat(path +"/"+ child).getMzxid() +"\t"+ zxid);
            if (getStat(path +"/"+ child).getMzxid() < zxid) {
                priorChildren.add(child);
            }
        }
        return priorChildren;
    }
    
    public Stat getStat(String path) throws KeeperException {
        try { 
            Stat    stat;
            
            stat = new Stat();
            getData(path, false, stat);
            return stat;
        } catch (InterruptedException ie) {
            throw new RuntimeException("panic");
        }
    }

    public ChildrenCallbackOp getChildrenAsync(String path, boolean watch) throws KeeperException {
        ChildrenCallbackOp  op;
        
        op = newChildrenCallbackOp();   
        super.getChildren(path, watch, (ChildrenCallback)this, op.getRequestUUID());
        return op;
    }

    public ChildrenCallbackOp getChildrenAsync(String path) throws KeeperException {
        return getChildrenAsync(path, false);
    }
    
    public void close() {
        try {
            super.close();
        } catch (InterruptedException ie) {
            throw new RuntimeException("panic");
        }
    }
    
    public static ZooKeeperExtended getZooKeeperWithRetries(ZooKeeperConfig zkConfig, int sessionTimeout, 
                                                       Watcher watcher, int connectAttempts) 
                                                       throws KeeperException, IOException {
        ZooKeeperExtended   _zk;
        int                 curAttempt;
        
        _zk = null;
        curAttempt = 1;
        do {
            try {
                _zk = new ZooKeeperExtended(zkConfig, sessionTimeout, watcher);
                while (_zk.getState() == States.CONNECTING) {
                    ThreadUtil.sleep(connectionCheckIntervalMillis);
                }
                break;
            } catch (IOException ioe) {
                Log.logErrorWarning(ioe);
                if (curAttempt >= connectAttempts) {
                    throw ioe;
                }
            } catch (KeeperException ke) {
                Log.logErrorWarning(ke);
                if (curAttempt >= connectAttempts) {
                    throw ke;
                }
            }
            curAttempt++;
            ThreadUtil.randomSleep(0, (1 << curAttempt) * 1000);
        } while (curAttempt < connectAttempts);
        return _zk;
    }
    
    public void createAllNodes(Collection<String> paths) throws KeeperException {
        for (String path : paths) {
            if (path != null) {
                createAllNodes(path);
            }
        }
    }
    
    public void createAllParentNodes(String path) throws KeeperException {
        int index;
        
        if (!path.startsWith("/")) {
            throw new RuntimeException("Bad path:"+ path);
        }
        index = path.lastIndexOf('/');
        if (index >= 0) {
            createAllNodes(path.substring(0, index));
        } else {
            // root level; no creation necessary
        }
    }
    
    public void createAllNodes(String path) throws KeeperException {
        int index;
        
        if (!path.startsWith("/")) {
            throw new RuntimeException("Bad path:"+ path);
        }
        index = 0;
        do {
            index = path.indexOf('/', index + 1);
            if (index < 0) {
                index = path.length();
            }
            try {
                create(path.substring(0, index));
            } catch (KeeperException ke) {
                if (ke.code() != Code.NODEEXISTS && ke.code() != Code.AUTHFAILED && ke.code() != Code.NOAUTH) {
                	Log.logErrorWarning(ke, "Error in create("+ path.substring(0, index) +")");
                    throw ke;
                }
            }
        } while (index < path.length());
    }
    
    public static String padVersion(long version) {
        return Strings.padStart(Long.toString(version), AUTO_VERSION_FIELD_SIZE, '0');
    }
    
    public static String padVersionPath(String path, long version) {
        return path +"/"+ padVersion(version);
    }
    
    public long getCreationTime(String path) throws KeeperException {
    	return getStat(path).getCtime();
    }
    
    public long getLatestVersion(String path) throws KeeperException {
        List<String>    children;
        List<Long>      currentVersions;
        
        children = getChildren(path);
        if (children.size() == 0) {
            return -1;
        } else {
            currentVersions = new ArrayList<>(children.size());
            for (String child : children) {
                currentVersions.add(Long.parseLong(child));
            }
            Collections.sort(currentVersions);
            return currentVersions.get(currentVersions.size() - 1);
        }
    }
    
    public long getLatestVersionFromPath(String path) {
        int index;
        
        index = path.lastIndexOf('/');
        return Long.parseLong(path.substring(index + 1));
    }

    public String getLatestVersionPath(String path) throws KeeperException {
        return path +"/"+ Strings.padStart(Long.toString(getLatestVersion(path)), 10, '0');
    }
    
    public long getVersionPriorTo(String path, long zxid) throws KeeperException {
        List<String>    children;
        List<Long>      currentVersions;
        
        //System.out.println("getVersionPriorTo "+ path +" "+ zxid);
        children = getChildrenPriorTo(path, zxid);
        if (children.size() == 0) {
            //System.out.println("no version prior to "+ path +" "+ zxid);
            return -1;
        } else {
            currentVersions = new ArrayList<>(children.size());
            for (String child : children) {
                currentVersions.add(Long.parseLong(child));
            }
            Collections.sort(currentVersions);
            return currentVersions.get(currentVersions.size() - 1);
        }
    }
    
    public static String parentPath(String childPath) {
        int     lastSlash;
        
        lastSlash = childPath.lastIndexOf('/');
        if (lastSlash < 0) {
            return null;
        } else {
            return childPath.substring(0, lastSlash);
        }
    }
    
    public static long getTrailingVersion(String path) {
        int     i;
        
        i = path.lastIndexOf('/');
        if (i < 0) {
            throw new RuntimeException("Invalid path: "+ path);
        } else {
            String  def;
            
            def = path.substring(i + 1);
            try {
                return Long.parseLong(def);
            } catch (NumberFormatException nfe) {
                throw new RuntimeException("Bad version: "+ def);
            }
        }
    }    
}

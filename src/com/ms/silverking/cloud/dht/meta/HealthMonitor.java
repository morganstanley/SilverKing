package com.ms.silverking.cloud.dht.meta;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.dht.management.LogStreamConfig;
import com.ms.silverking.cloud.meta.ChildrenListener;
import com.ms.silverking.cloud.meta.ChildrenWatcher;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.cloud.toporing.InstantiatedRingTree;
import com.ms.silverking.cloud.toporing.ResolvedReplicaMap;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.time.TimeUtils;

public class HealthMonitor implements ChildrenListener, DHTMetaUpdateListener {
    private final ZooKeeperConfig   zkConfig;
    private final MetaClient    mc;
    private final SuspectsZK    suspectsZK;
    private final DaemonStateZK	daemonStateZK;
    private final int			watchIntervalSeconds;
    private ChildrenWatcher   watcher;
    private final DHTMetaWatcher    dmw;
    private final int           guiltThreshold;
    private boolean running;
    private InstanceExclusionZK	instanceExclusionZK;
	private DHTRingCurTargetZK	dhtRingCurTargetZK;	
    private Set<IPAndPort>  activeNodes;
    private Lock	checkMutex;
    private final Doctor	doctor;
    private final DoctorRunner	doctorRunner;
    private final int doctorRoundIntervalSeconds;
    private final boolean	forceInclusionOfUnsafeExcludedServers;
    private final ConvictionLimits	convictionLimits;
    private final Map<IPAndPort,Long>	convictionTimes;
    private volatile long	lastCheckMillis;
    private Set<IPAndPort>	activeNodesInMap;
    private final boolean	disableAddition;
    
    private static final String	logFileName = "HealthMonitor.out";
    private static final String	doctorThreadName = "DoctorRunner";
	    
    //private static final int    dmwUpdateIntervalMillis = 5 * 1000; // for testing
    private static final int    dmwUpdateIntervalMillis = 1 * 60 * 1000;
    //private static final int    inactiveNodeMarkingThreshold_servers = 1;
    //private static final double inactiveNodeMarkingThreshold_fraction = 0.05;
    private static final int    forcedCheckIntervalMillis = 1 * 60 * 1000;
    
    private static final long	oneHourMillis = TimeUtils.MINUTES_PER_HOUR * TimeUtils.SECONDS_PER_MINUTE * TimeUtils.MILLIS_PER_SECOND;

    
    // FUTURE: just pass in the options...
    public HealthMonitor(SKGridConfiguration gc, ZooKeeperConfig zkConfig, int watchIntervalSeconds, int guiltThreshold, 
    					 int doctorRoundIntervalSeconds, boolean forceInclusionOfUnsafeExcludedServers,
    					 ConvictionLimits convictionLimits, int doctorNodeStartupTimeoutSeconds,
    					 boolean disableAddition)
                         throws IOException, KeeperException {
    	String	dhtName;
    	
    	dhtName = gc.getClientDHTConfiguration().getName();
        this.guiltThreshold = guiltThreshold;
        this.zkConfig = zkConfig;
        this.watchIntervalSeconds = watchIntervalSeconds;
        this.doctorRoundIntervalSeconds = doctorRoundIntervalSeconds;
        this.forceInclusionOfUnsafeExcludedServers = forceInclusionOfUnsafeExcludedServers;
        this.convictionLimits = convictionLimits;
        convictionTimes = new HashMap<>();
        this.disableAddition = disableAddition;
        
        if (doctorRoundIntervalSeconds != HealthMonitorOptions.NO_DOCTOR) {
        	doctor = new Doctor(gc, forceInclusionOfUnsafeExcludedServers, doctorNodeStartupTimeoutSeconds);
        } else {
        	doctor = null;
        }
        activeNodes = ImmutableSet.of();
        checkMutex = new ReentrantLock();
        mc = new MetaClient(dhtName, zkConfig);
        suspectsZK = new SuspectsZK(mc);    
        daemonStateZK = new DaemonStateZK(mc);
        dmw = new DHTMetaWatcher(zkConfig, dhtName, dmwUpdateIntervalMillis);
        dmw.addListener(this);
        if (doctor != null) {
	        doctorRunner = new DoctorRunner();
	        doctorRunner.start();
        } else {
        	doctorRunner = null;
        }
    }
    
    @Override
    public void dhtMetaUpdate(DHTMetaUpdate dhtMetaUpdate) {
        Log.warning(String.format("Received dhtMetaUpdate %s", dhtMetaUpdate));
        try {
            InstantiatedRingTree	rawRingTree;
            ResolvedReplicaMap		replicaMap;
            
    		instanceExclusionZK = new InstanceExclusionZK(dhtMetaUpdate.getMetaClient());
    		dhtRingCurTargetZK = new DHTRingCurTargetZK(dhtMetaUpdate.getMetaClient(), dhtMetaUpdate.getDHTConfig());
            rawRingTree = dhtMetaUpdate.getRingTree();
            replicaMap = rawRingTree.getResolvedMap(dhtMetaUpdate.getNamedRingConfiguration().getRingConfiguration().getRingParentName(), null);
            activeNodesInMap = replicaMap.allReplicas();
        	synchronized (this) {
        		this.notifyAll();
        	}
        } catch (Exception e) {
            Log.logErrorWarning(e, "Exception in HealthMonitor.dhtMetaUpdate()");
        }
    }

    @Override
    public void childrenChanged(String basePath, Map<String, byte[]> childStates) {
        Log.warning(String.format("\nchildrenChanged\n"));
        for (Map.Entry<String,byte[]> entry : childStates.entrySet()) {
            Log.warning(String.format("%s\t%s", entry.getKey(), new String(entry.getValue())));
        }
        check();
    }
    
    private void verifyEligibility(Set<IPAndPort> nodes) throws KeeperException {
    	if (nodes.size() > 0) {
        	int			port;
        	Set<String>	newlyInactiveServers;
        	Set<String>	ineligibleServers;
        	
    		port = nodes.iterator().next().getPort();    		        	
        	newlyInactiveServers = IPAndPort.copyServerIPsAsMutableSet(nodes);
        	ineligibleServers = removeIneligibleServers(newlyInactiveServers, dhtRingCurTargetZK, instanceExclusionZK);
        	for (String ineligibleServer : ineligibleServers) {
        		IPAndPort	ineligibleNode;
        		
        		ineligibleNode = new IPAndPort(ineligibleServer, port);
        		newlyInactiveServers.remove(ineligibleNode);
        	}
    	}
    }
    
    public static Set<String> removeIneligibleServers(Set<String> servers, DHTRingCurTargetZK _dhtRingCurTargetZK, 
    									 InstanceExclusionZK _instanceExclusionZK) throws KeeperException {
    	Stat	stat;
    	long	curRingZmxid;
    	Set<String>	ineligibleServers;
    	Map<String,Long>	esStarts;
    	
    	stat = new Stat();
    	_dhtRingCurTargetZK.getCurRingAndVersionPair(stat);
    	curRingZmxid = stat.getMzxid();
    	ineligibleServers = new HashSet<>();
    	
    	esStarts = _instanceExclusionZK.getStartOfCurrentExclusion(servers);

    	for (String server : servers) {
    		long	startOfCurrentExclusion;
    		long	startOfCurrentExclusionMzxid;
    		
    		startOfCurrentExclusion = esStarts.get(server);
    		if (startOfCurrentExclusion < 0) {
    			Log.warning("Unexpected can't find startOfCurrentExclusion for ", server);
    		} else {
	    		startOfCurrentExclusionMzxid = _instanceExclusionZK.getVersionMzxid(startOfCurrentExclusion);
	    		if (startOfCurrentExclusionMzxid > curRingZmxid) {
	    			Log.warningf("Ineligible: %s %d > %d", server, startOfCurrentExclusionMzxid, curRingZmxid);
	    			ineligibleServers.add(server);
	    		}
        	}
    	}
    	servers.removeAll(ineligibleServers);
    	return ineligibleServers;
	}    
    
    public void check() {
    	lastCheckMillis = SystemTimeUtil.systemTimeSource.absTimeMillis();
    	checkMutex.lock();
        try {
            SetMultimap<IPAndPort,IPAndPort>    accuserSuspects;
            SetMultimap<IPAndPort,IPAndPort>    suspectAccusers;
            Set<IPAndPort>  guiltySuspects;
            Set<IPAndPort>  newActiveNodes;
            Set<IPAndPort>  newlyInactiveNodes;
            
            guiltySuspects = new HashSet<>();
            
            accuserSuspects = suspectsZK.readAccuserSuspectsFromZK();
            suspectAccusers = CollectionUtil.transposeSetMultimap(accuserSuspects);
            // Read the current active nodes from ZK
            newActiveNodes = suspectsZK.readActiveNodesFromZK();
            // Now compute newlyInactiveNodes as the set difference of the previously active nodes minus the active nodes in ZK
            newlyInactiveNodes = new HashSet<>(activeNodes); 
            newlyInactiveNodes.removeAll(newActiveNodes);
            filterPassiveNodes(newlyInactiveNodes);
            // verify that all newly active nodes are eligible to return
            //verifyEligibility(newlyInactiveNodes);
            verifyEligibility(newActiveNodes);
            // Store activeNodes for the next computation
            activeNodes = newActiveNodes;
            
            Log.warning(String.format("newlyInactiveNodes\t%s", CollectionUtil.toString(newlyInactiveNodes)));
            
            // First add newlyInactiveNodes to our guiltySuspects set
            // Throttle the inactive marking
            // FUTURE - improve/remove this
            /*
            if (newlyInactiveNodes.size() <= inactiveNodeMarkingThreshold_servers) {
                guiltySuspects.addAll(newlyInactiveNodes);
            } else if ((double)newlyInactiveNodes.size() / (double)activeNodes.size() 
                                                    <= inactiveNodeMarkingThreshold_fraction) {
                guiltySuspects.addAll(newlyInactiveNodes);
            } else {
                Log.warning("Not marking newly inactive nodes "+ newlyInactiveNodes.size());
            }
            */
            guiltySuspects.addAll(newlyInactiveNodes);
            
            // Now look through the list of suspects for additional nodes to add to guiltySuspects
            for (IPAndPort suspect : suspectAccusers.keySet()) {
                Set<IPAndPort>  accusers;
                
                accusers = suspectAccusers.get(suspect);
                // First check to see if this suspect has maintained its ephemeral node
                // in the suspects list. If it has not, then we presume that the loss of
                // the ephemeral node + the suspicion by another is sufficient to
                // prove that this node is bad.
                if (!accuserSuspects.containsKey(suspect)) {
                    Log.warning(String.format("Guilty 1: %s (at least one accuser, and no ephemeral node)", suspect));
                    guiltySuspects.add(suspect);
                } else {
                    Log.warning(String.format("suspectAccusers contains suspect %s, maps to %s", 
                            suspect, suspectAccusers.get(suspect)));
                    if (accusers.size() >= guiltThreshold) {
                        Log.warning(String.format("Guilty 2 %s (accusers.size() %d >= guiltThreshold %d)", suspect, accusers.size(), guiltThreshold));
                        guiltySuspects.add(suspect);
                    } else {
                        Log.warning(String.format("Not guilty 3 %s (accusers.size() %d < guiltThreshold %d)", suspect, accusers.size(), guiltThreshold));
                    }
                }
            }
            
            if (!disableAddition) {
            	removeFromConvictionTimes(newActiveNodes);
            }
            if (guiltySuspects.size() > convictionLimits.getTotalGuiltyServers()) {
            	Log.warning("guiltySuspects.size() > convictionLimits.getTotalGuiltyServers()");
            	Log.warningf("%d > %d", guiltySuspects.size(), convictionLimits.getTotalGuiltyServers());
            } else {
            	int	convictionsWithinOneHour;
            	
            	convictionsWithinOneHour = getConvictionsWithinTimeWindow(oneHourMillis);            	
            	if (convictionsWithinOneHour > convictionLimits.getGuiltyServersPerHour()) {
                	Log.warning("convictionsWithinOneHour > convictionLimits.getGuiltyServersPerHour()");
                	Log.warningf("%d > %d", convictionsWithinOneHour, convictionLimits.getGuiltyServersPerHour());
            	} else {
	            	addToConvictionTimes(guiltySuspects, SystemTimeUtil.systemTimeSource.absTimeMillis());
		            if (doctor != null) {
		            	doctor.admitPatients(guiltySuspects);
		                if (!disableAddition) {
		                	doctor.releasePatients(newActiveNodes);
		                }
		            }
		            
		            // FIXME - We need to check if the newActiveNodes are either in the current or target ring. If so, we
		            // remove them so that they are not activated.
		            // We can't allow them to become active as this could result in data loss. 
		            
		            updateInstanceExclusionSet(guiltySuspects, disableAddition ? ImmutableSet.of() : newActiveNodes);
            	}
            }
        } catch (Exception e) {
            Log.logErrorWarning(e);
        } finally {
        	checkMutex.unlock();
        }
    }
    
	private void filterPassiveNodes(Set<IPAndPort> nodes) {
		Set<IPAndPort>	passiveNodes;
		
		passiveNodes = new HashSet<>();
		for (IPAndPort node : nodes) {
			if (!activeNodesInMap.contains(node)) {
				Log.warning("Ignoring passive node: "+ node);
				passiveNodes.add(node);
			}
		}
		nodes.removeAll(passiveNodes);
	}

	private void removeFromConvictionTimes(Set<IPAndPort> healthServers) {
		for (IPAndPort server : healthServers) {
			convictionTimes.remove(server);
		}
	}
	
	private void addToConvictionTimes(Set<IPAndPort> guiltySuspects, long absTimeMillis) {
		for (IPAndPort suspect : guiltySuspects) {
			if (!convictionTimes.containsKey(suspect)) {
				convictionTimes.put(suspect, absTimeMillis);
			}
		}
	}
	
	private int getConvictionsWithinTimeWindow(long relTimeMillis) {
		long	windowStart;
		int		totalConvictions;
		
		windowStart = SystemTimeUtil.systemTimeSource.absTimeMillis() - relTimeMillis;
		totalConvictions = 0;
		for (long convictionTimeMillis : convictionTimes.values()) {
			if (convictionTimeMillis > windowStart) {
				++totalConvictions;
			}
		}
		return totalConvictions;
	}

	private Set<String> hostStringSet(Set<IPAndPort> s) {
        ImmutableSet.Builder<String>    ss;
        
        ss = ImmutableSet.builder();
        for (IPAndPort ipAndPort : s) {
            ss.add(ipAndPort.getIPAsString());
        }
        return ss.build();
    }
    
    private void updateInstanceExclusionSet(Set<IPAndPort> guiltySuspects, Set<IPAndPort> newActiveNodes) {
        try {
            if (instanceExclusionZK != null) { 
                ExclusionSet    oldExclusionSet;
                ExclusionSet    newExclusionSet;
                
                if (!guiltySuspects.isEmpty()) {
                    Log.warning(String.format("Marking as bad"));
                    Log.warning(String.format("%s\n", 
                            CollectionUtil.toString(hostStringSet(guiltySuspects))));
                } else {
                    Log.warning(String.format("No guilty suspects"));
                }
                if (!newActiveNodes.isEmpty()) {
                    Log.warning(String.format("Marking as good"));
                    Log.warning(String.format("%s\n", 
                            CollectionUtil.toString(hostStringSet(newActiveNodes))));
                } else {
                    Log.warning(String.format("No newly good nodes"));
                }
                
                Log.warning(String.format("Latest exclusion set path %s", instanceExclusionZK.getLatestZKPath()));
                if (instanceExclusionZK.getLatestZKPath() != null) {
                	oldExclusionSet = instanceExclusionZK.readLatestFromZK();
                } else {
                	oldExclusionSet = ExclusionSet.emptyExclusionSet(0);
                }
                newExclusionSet = oldExclusionSet.add(hostStringSet(guiltySuspects))
                                                 .remove(hostStringSet(newActiveNodes));
                // window of vulnerability here
                // for now we ensure this isn't violated externally
                Log.warning(String.format("Old exclusion set %d %s", oldExclusionSet.size(), oldExclusionSet));
                Log.warning(String.format("New exclusion set %d %s", newExclusionSet.size(), newExclusionSet));
                if (!newExclusionSet.equals(oldExclusionSet)) {
                    Log.warning(String.format("Writing exclusion set"));
                    instanceExclusionZK.writeToZK(newExclusionSet);
                    Log.warning(String.format("Latest exclusion set path after write %s", instanceExclusionZK.getLatestZKPath()));
                } else {
                    Log.warning(String.format("No change in exclusion set"));
                }
            } else {
                Log.warning(String.format("Unable to mark as bad as exclusionZK is null"));
            }
        } catch (Exception e) {
            Log.logErrorWarning(e, "Exception in updateInstanceExclusionSet");
        }
    }

    public void monitor() {
    	waitForDHTMetaUpdate();
        watcher = new ChildrenWatcher(mc, mc.getMetaPaths().getInstanceSuspectsPath(), 
                this, watchIntervalSeconds * 1000);
        running = true;
        while (running) {
            ThreadUtil.sleep(1000);
            if (SystemTimeUtil.systemTimeSource.absTimeMillis() - lastCheckMillis > forcedCheckIntervalMillis) {
            	Log.warning("Forcing check()");
                check();
            }
        }
    }
    
    private void waitForDHTMetaUpdate() {
    	Log.warning("in waitForDHTMetaUpdate");
    	synchronized (this) {
    		while (instanceExclusionZK == null) {
    			try {
					this.wait();
				} catch (InterruptedException e) {
				}
    		}
    	}
    	Log.warning("out waitForDHTMetaUpdate");
	}
    
    
    //////////////////////////
    
    class DoctorRunner implements Runnable {
    	private boolean	running;
    	
    	DoctorRunner() {
    	}
    	
    	void start() {
    		synchronized (this) {
    			if (!running) {
		    		running = true;
		    		Log.warning("Starting "+ doctorThreadName);
		    		new Thread(this, doctorThreadName).start();
    			}
    		}
    	}
    	
    	void stop() {
    		Log.warning("Stopping "+ doctorThreadName);
    		running = false;
    	}

		@Override
		public void run() {
			try {
				while (running) {
					if (dhtRingCurTargetZK == null) {
						Log.warning("Doctor skipping rounds due to no dhtRingCurTargetZK");
					} else {
						if (dhtRingCurTargetZK.curAndTargetRingsMatch()) {
							Log.warning("Doctor will make rounds as cur and target rings match");
							Log.warning("Doctor acquiring checkMutex");
					    	checkMutex.lock();
					        try {
								Log.warning("Doctor is making rounds");
					        	doctor.makeRounds();
								Log.warning("Doctor is done making rounds");
					        } finally {
					        	checkMutex.unlock();
								Log.warning("Doctor releasing checkMutex");
					        }
						} else {
							Log.warning("Doctor is skipping rounds as cur and target rings do not match (convergence is ongoing)");
						}
					}
					Log.warning("Doctor is sleeping");
					ThreadUtil.sleepSeconds(doctorRoundIntervalSeconds);
					Log.warning("Doctor is awake");
				}
			} catch (Exception e) {
				Log.logErrorWarning(e);
			} finally {
	    		Log.warning("Exiting "+ doctorThreadName);
			}
		}
    }
    
    //////////////////////////

	public static void main(String[] args) {
        try {
            CmdLineParser       parser;
            HealthMonitorOptions    options;
            HealthMonitor   healthMonitor;
            SKGridConfiguration gc;
            
            options = new HealthMonitorOptions();
            parser = new CmdLineParser(options);
            try {
            	ConvictionLimits	convictionLimits;
            	
                parser.parseArgument(args);
                gc = SKGridConfiguration.parseFile(options.gridConfig);
            	convictionLimits = ConvictionLimits.parse(options.convictionLimits);
                LogStreamConfig.configureLogStreams(gc, logFileName);
                healthMonitor = new HealthMonitor(gc, 
                                                  gc.getClientDHTConfiguration().getZKConfig(), 
                                                  options.watchIntervalSeconds,
                                                  options.guiltThreshold,
                                                  options.doctorRoundIntervalSeconds,
                                                  options.forceInclusionOfUnsafeExcludedServers,
                                                  convictionLimits,
                                                  options.doctorNodeStartupTimeoutSeconds,
                                                  options.disableAddition);
                healthMonitor.monitor();
            } catch (CmdLineException cle) {
                System.err.println(cle.getMessage());
                parser.printUsage(System.err);
	            System.exit(-1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}

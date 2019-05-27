package com.ms.silverking.cloud.meta.test;

import java.util.Map;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.dht.meta.InstanceExclusionZK;
import com.ms.silverking.cloud.dht.meta.MetaClient;

public class StartOfCurrentExclusionTest {
    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            if (args.length < 2) {
                System.out.println("<gcName> <server>");
            } else {
            	InstanceExclusionZK	ieZK;
            	MetaClient			mc;
            	String				gcName;
            	String				server;
            	Map<String,Long>	exclusionSetStartMap;
            	long				v;
            	
            	gcName = args[0];
            	server = args[1];
            	mc = new MetaClient(SKGridConfiguration.parseFile(gcName));
            	ieZK = new InstanceExclusionZK(mc);
            	exclusionSetStartMap = ieZK.getStartOfCurrentExclusion(ImmutableSet.of(server));
            	v = exclusionSetStartMap.get(server);
            	System.out.printf("StartOfCurrentExclusionTest: %s\n", v);
            	System.out.printf("mzxid: %d\n", ieZK.getVersionMzxid(v));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}

package com.ms.silverking.cloud.dht.gridconfig;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.client.ClientDHTConfigurationProvider;
import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.cloud.gridconfig.GridConfiguration;

/**
 * Provides coherent, centralized configuration for SilverKing settings.
 */
public class SKGridConfiguration extends GridConfiguration implements Serializable, ClientDHTConfigurationProvider {
    private final ClientDHTConfiguration   dhtConfig;
    private final String				skfsConfigName;
	
    private static final long serialVersionUID = 5006385397693770223L;
    
	private static final String	skfsConfigNameVar = "GC_SKFS_CONFIG";
    
    public SKGridConfiguration(String name, Map<String,String> envMap) {
	    super(name, envMap);
		this.dhtConfig = ClientDHTConfiguration.create(envMap);
		this.skfsConfigName = envMap.get(skfsConfigNameVar);
	}
		
    @OmitGeneration
    public SKGridConfiguration(GridConfiguration gc) {
        super(gc.getName(), gc.getEnvMap());
        this.dhtConfig = ClientDHTConfiguration.create(gc.getEnvMap());
		this.skfsConfigName = gc.getEnvMap().get(skfsConfigNameVar);
    }
    
	public static SKGridConfiguration parseFile(File gcBase, String gcName) throws IOException {
	    GridConfiguration  gc;
	    
	    gc = GridConfiguration.parseFile(gcBase, gcName);
		return new SKGridConfiguration(gc);
	}
	
	public static SKGridConfiguration parseFile(String gcName) throws IOException {
        GridConfiguration  gc;
        
        gc = GridConfiguration.parseFile(gcName);
        return new SKGridConfiguration(gc);
	}
	
    @Override
    public ClientDHTConfiguration getClientDHTConfiguration() {
        return dhtConfig;
    }
    
    public String getSKFSConfigName() {
    	return skfsConfigName;
    }
	
	public String toString() {
		StringBuilder	sb;
		
		sb = new StringBuilder();
		sb.append(super.toString());
		sb.append(':');
		sb.append(dhtConfig.toString());
		return sb.toString();
	}
}

package com.ms.silverking.cloud.toporing.meta;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.management.MetaToolModuleBase;
import com.ms.silverking.cloud.management.MetaToolOptions;

public class WeightsZK extends MetaToolModuleBase<WeightSpecifications,MetaPaths> {
    // Below must agree with WeightSpecifications.parse
    // FUTURE - make code common
    private static final char   fieldDelimiterChar = '\t';
    private static final String fieldDelimiterString = "" + fieldDelimiterChar;
    private static final char   entryDelimiterChar = '\n';
    private static final String entryDelimiterString = "" + entryDelimiterChar;
    
    public WeightsZK(MetaClient mc) throws KeeperException {
        super(mc, mc.getMetaPaths().getWeightsPath());
    }
    
    @Override
    public WeightSpecifications readFromFile(File file, long version) throws IOException {
        return (WeightSpecifications)new WeightSpecifications(version).parse(file);
    }

    @Override
    public WeightSpecifications readFromZK(long version, MetaToolOptions options) throws KeeperException {
        String  vBase;
        ByteArrayInputStream    inStream;
        
        vBase = getVBase(version);
        inStream = new ByteArrayInputStream(zk.getString(vBase).getBytes());
        try {
            return (WeightSpecifications)new WeightSpecifications(version).parse(inStream);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
        /*
        String  vBase;
        List<String>    nodes;
        Map<String, Double> nodeWeights;

        nodeWeights = new HashMap<>();
        vBase = getVBase(version);
        nodes = zk.getChildren(vBase);
        for (String node : nodes) {
            double  weight;
            
            weight = zk.getDouble(vBase +"/"+ node);
            nodeWeights.put(node, weight);
        }
        return new WeightSpecifications(version, nodeWeights);
        */
    }
    
    @Override
    public void writeToFile(File file, WeightSpecifications instance) throws IOException {
        throw new RuntimeException("writeToFile not implemented for WeightSpecifications");
    }

    @Override
    public String writeToZK(WeightSpecifications weightSpecs, MetaToolOptions options) throws IOException, KeeperException {
        String  vBase;
        StringBuilder   sb;

        sb = new StringBuilder();
        for (Map.Entry<String,Double> nodeWeight : weightSpecs.getNodeWeights()) {
            sb.append(nodeWeight.getKey() + fieldDelimiterChar + nodeWeight.getValue() + entryDelimiterChar);
        }
        // trim trailing entryDelimiter
        if (sb.length() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }
        vBase = zk.createString(base +"/" , sb.toString(), CreateMode.PERSISTENT_SEQUENTIAL);
        /*
        String  vBase;

        vBase = zk.createString(base +"/" , "", CreateMode.PERSISTENT_SEQUENTIAL);
        for (Map.Entry<String,Double> nodeWeight : weightSpecs.getNodeWeights()) {
            zk.createDouble(vBase +"/"+ nodeWeight.getKey(), nodeWeight.getValue());
        }
        */
        return null;        
    }    
}

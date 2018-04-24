package com.ms.silverking.cloud.dht.daemon.storage.convergence;

import java.util.ArrayList;
import java.util.List;

import com.ms.silverking.cloud.dht.daemon.storage.KeyAndVersionChecksum;

/**
 * Results of a checksum tree comparison.
 */
public class MatchResult {
    private final List<KeyAndVersionChecksum>    sourceNotInDest;
    private final List<KeyAndVersionChecksum>    destNotInSource;
    private final List<KeyAndVersionChecksum>    checksumMismatch;
    
    public MatchResult() {
        sourceNotInDest = new ArrayList<>();
        destNotInSource = new ArrayList<>();
        checksumMismatch = new ArrayList<>();
    }
    
    public void addSourceNotInDest(KeyAndVersionChecksum kvc) {
        sourceNotInDest.add(kvc);
    }
    
    public void addDestNotInSource(KeyAndVersionChecksum kvc) {
        destNotInSource.add(kvc);
    }
    
    public void addChecksumMismatch(KeyAndVersionChecksum kvc) {
        checksumMismatch.add(kvc);
    }
    
    public void add(MatchResult other) {
        sourceNotInDest.addAll(other.sourceNotInDest);
        destNotInSource.addAll(other.destNotInSource);
        checksumMismatch.addAll(other.checksumMismatch);
    }
    
    public List<KeyAndVersionChecksum> getSourceNotInDest() {
        return sourceNotInDest;
    }

    public List<KeyAndVersionChecksum> getDestNotInSource() {
        return destNotInSource;
    }

    public List<KeyAndVersionChecksum> getChecksumMismatch() {
        return checksumMismatch;
    }
    
    public boolean isNonEmpty() {
        return sourceNotInDest.size() > 0 || destNotInSource.size() > 0 || checksumMismatch.size() != 0;
    }
    
    public boolean perfectMatch() {
        return sourceNotInDest.size() == 0
                && destNotInSource.size() == 0
                && checksumMismatch.size() == 0;
    }
    
    public String toSummaryString() {
    	return String.format("sourceNotInDest %d destNotInSource %d checksumMismatch %d", sourceNotInDest.size(), destNotInSource.size(), checksumMismatch.size());
    }
    
    @Override
    public String toString() {
        StringBuilder   sb;
        
        sb = new StringBuilder();
        if (perfectMatch()) {
            sb.append("perfect match\n");
        } else {
            addList(sb, sourceNotInDest, "sourceNotInDest");
            addList(sb, destNotInSource, "destNotInSource");
            addList(sb, checksumMismatch, "checksumMismatch");
        }
        return sb.toString();
    }

    private void addList(StringBuilder sb, List<KeyAndVersionChecksum> list, String listName) {
        if (list.size() == 0) {
            //sb.append("\t<empty>\n");
        } else {
            sb.append(listName);
            sb.append('\n');
            for (KeyAndVersionChecksum kvc : list) {
                sb.append('\t');
                sb.append(kvc);
                sb.append('\n');
            }
        }
    }
}

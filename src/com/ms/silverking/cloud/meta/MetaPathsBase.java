package com.ms.silverking.cloud.meta;

import java.util.List;

/**
 * Common meta path functionality.
 */
public abstract class MetaPathsBase {
    protected List<String> pathList;
    
    // cloud global base directories
    public static final String  cloudGlobalBase = "/cloud";
    
    public MetaPathsBase() {
    }
    
    public List<String> getPathList() {
        return pathList;
    }
}

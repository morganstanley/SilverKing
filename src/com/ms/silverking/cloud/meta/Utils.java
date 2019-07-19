package com.ms.silverking.cloud.meta;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

public class Utils {
    public static final Set<String> optionalVersionFieldSet;
    
    static {
        ImmutableSet.Builder<String>    builder;
        
        builder = ImmutableSet.builder();
        optionalVersionFieldSet = builder.add("version").build();
    }
}

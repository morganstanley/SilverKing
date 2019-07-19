package com.ms.silverking.cloud.common;

public enum OwnerQueryMode {
    Primary, Secondary, All;
    
    public boolean includePrimary() {
        switch (this) {
        case Primary: return true;
        case Secondary: return false;
        case All: return true;
        default: throw new RuntimeException("Panic");
        }
    }
    
    public boolean includeSecondary() {
        switch (this) {
        case Primary: return false;
        case Secondary: return true;
        case All: return true;
        default: throw new RuntimeException("Panic");
        }
    }
}

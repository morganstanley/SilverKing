package com.ms.silverking.util;

public enum Mutability {
    Mutable, Immutable;
    
    public void ensureMutable() {
        if (this != Mutable) {
            throw new RuntimeException("ensureMutable() failed");
        }
    }
    
    public void ensureImmutable() {
        if (this != Immutable) {
            throw new RuntimeException("ensureImmutable() failed");
        }
    }
}

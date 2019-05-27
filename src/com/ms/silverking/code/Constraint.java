package com.ms.silverking.code;

/**
 * Simplifies constraint checking. Similar to assertion except that constraints are not
 * disabled. Also similar to Guava Precondition class except that this is intended
 * for general usage, not just method preconditions. 
 */
public class Constraint {
    public static void ensureNotNull(Object o) {
        if (o == null) {
            throw new ConstraintViolationException("ensureNotNull failed");
        }
    }
    
    public static void ensureNull(Object o) {
        if (o != null) {
            throw new ConstraintViolationException("ensureNull failed: "+ o);
        }
    }
    
    public static void ensureNotEqual(int i1, int i2) {
        if (i1 != i2) {
            throw new ConstraintViolationException("ensureNotEqual failed: "+ i1 +" "+ i2);
        }        
    }
    
    public static void ensureNonZero(int i) {
        if (i == 0) {
            throw new ConstraintViolationException("ensureNonZero failed: "+ i);
        }        
    }
    
    /**
     * Check bounds inclusive
     * @param min
     * @param max
     * @param x
     */
    public static void checkBounds(int min, int max, int i) {
        if (i < min) {
            throw new ConstraintViolationException("checkBounds min bound failed: "+ i +" < "+ min);
        } else if (i > max) {
            throw new ConstraintViolationException("checkBounds max bound failed: "+ i +" > "+ max);
        }
    }
}

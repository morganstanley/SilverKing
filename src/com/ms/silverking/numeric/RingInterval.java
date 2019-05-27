package com.ms.silverking.numeric;

public class RingInterval {
    private final RingInteger   min;
    private final RingInteger   max;
    
    public RingInterval(RingInteger min, RingInteger max) {
        this.min = min;
        this.max = max;
    }
    
    public boolean contains(RingInteger x) {
        RingInteger.ensureRingShared(min, max);
        RingInteger.ensureRingShared(min, x);
        
        if (max.value > min.value) {
            return x.value >= min.value && x.value <= max.value;
        } else if (max.value < min.value) {
            return x.value <= max.value || x.value >= min.value;
        } else {
            return x.value == max.value;
        }
    }
}

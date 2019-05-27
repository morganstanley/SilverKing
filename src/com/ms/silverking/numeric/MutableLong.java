package com.ms.silverking.numeric;

public class MutableLong {
    private long value;
    
    public MutableLong(long value) {
        this.value = value;
    }
    
    public MutableLong() {
        this(0);
    }
    
    public long getValue() {
        return value;
    }
    
    public void increment() {
        value++;
    }
    
    public void decrement() {
        value--;
    }
    
    public long getAndIncrement() {
        return value++;
    }
    
    public long getAndDecrement() {
        return value--;
    }
    
    @Override
    public int hashCode() {
        return (int)value;
    }
    
    @Override
    public String toString() {
        return Long.toString(value);
    }
}

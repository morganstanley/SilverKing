package com.ms.silverking.cloud.dht.collection;


interface ValueTable {
    public final int   noMatch = Integer.MIN_VALUE;
    
    public int add(long msl, long lsl, int value);
    public void store(int index, long msl, long lsl, int value);
    public int matches(int index, long msl, long lsl);
    public long getMSL(int index);
    public long getLSL(int index);
    public int getValue(int index);
    public void clear();
    public int getSizeBytes();
}

package com.ms.silverking.util;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.base.Preconditions;

public class Arrays {
    public static boolean matchesRegion(byte[] a1, int offset1, byte[] a2, int offset2, int length) {
        for (int i = 0; i < length; i++) {
            if (a1[offset1 + i] != a2[offset2 + i]) {
                return false;
            }
        }
        return true;
    }
    
    public static boolean matchesStart(byte[] a1, byte[] a2) {
        for (int i = 0; i < a1.length; i++) {
            if (a1[i] != a2[i]) {
                return false;
            }
        }
        return true;
    }
    
    public static <T> int indexOf(T[] a1, T v) {
        for (int i = 0; i < a1.length; i++) {
            if (a1[i].equals(v)) {
                return i;
            }
        }
        return -1;
    }
    
    public static <T> boolean contains(T[] a1, T v) {
    	return indexOf(a1, v) >= 0;
    }
    
    public static void shuffleIntArray(int[] a){
        shuffleIntArray(a, ThreadLocalRandom.current());
    }
    
    public static void shuffleIntArray(int[] a, Random random){
        int index;

        for (int i = a.length - 1; i > 0; i--) {
            index = random.nextInt(i + 1);
            if (index != i) {
                a[index] ^= a[i];
                a[i] ^= a[index];
                a[index] ^= a[i];
            }
        }
    }

    public static int[] randomUniqueIntArray(int max) {
        return randomUniqueIntArray(max, ThreadLocalRandom.current());
    }
    
    public static int[] randomUniqueIntArray(int max, Random random) {
        return randomUniqueIntArray(0, max, random);
    }
    
    public static int[] randomUniqueIntArray(int min, int max) {
        return randomUniqueIntArray(min, max, ThreadLocalRandom.current());
    }

    /**
     * @param min
     * @param max max exclusive
     * @param random
     * @return
     */
    public static int[] randomUniqueIntArray(int min, int max, Random random) {
        int[]   x;
        int     size;
        
        Preconditions.checkArgument(min >= 0, "min must be non-negative");
        Preconditions.checkArgument(max >= 0, "max must be non-negative");
        Preconditions.checkArgument(max > min, "max must be > min");
        size = max - min;
        x = new int[size];
        for (int i = 0; i < x.length; i++) {
            x[i] = min + i;
        }
        Arrays.shuffleIntArray(x, random);
        return x;
    }    
}


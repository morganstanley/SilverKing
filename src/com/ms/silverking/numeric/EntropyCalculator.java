package com.ms.silverking.numeric;

public class EntropyCalculator {
    public static double computeEntropy(byte[] b) {
        return computeEntropy(b, 0, b.length);
    }
    
    public static double computeEntropy(byte[] b, int offset, int length) {
        long[]  occurrences;
        double  sum;
        
        occurrences = new long[NumConversion.BYTE_MAX_UNSIGNED_VALUE + 1];
        for (int i = 0; i < length; i++) {
            occurrences[NumConversion.byteToPositiveInt(b[offset + i])]++;
        }
        sum = 0.0;
        for (int i = 0; i < occurrences.length; i++) {
            if (occurrences[i] > 0) {
                double  p;
                double  lp;
                
                p = (double)occurrences[i] / (double)length;
                lp = NumUtil.log(2.0, p);
                sum += p * lp;
            }
        }
        return -sum;
    }
}

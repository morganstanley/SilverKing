package com.ms.silverking.net.analysis.iperf;

public class SummaryStat {
    private final long  total;
    private final long  mean;
    private final long  median;
    private final long  max;
    private final long  min;
    
    public static String[]  stats = {"total", "mean", "median", "max", "min"};
    
    public SummaryStat(long total, long mean, long median, long max, long min) {
        this.total = total;
        this.mean = mean;
        this.median = median;
        this.max = max;
        this.min = min;
    }
    
    public long getStat(String stat) {
        switch (stat) {
        case "total": return getTotal();
        case "mean": return getMean();
        case "median": return getMedian();
        case "max": return getMax();
        case "min": return getMin();
        default: throw new RuntimeException("Unknown stat: "+ stat);
        }
    }
    
    public long getTotal() {
        return total;
    }

    public long getMean() {
        return mean;
    }

    public long getMedian() {
        return median;
    }

    public long getMax() {
        return max;
    }

    public long getMin() {
        return min;
    }
}
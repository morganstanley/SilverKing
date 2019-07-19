package com.ms.silverking.net.analysis.iperf;

public class TimeInterval {
    private final double  start;
    private final double  end;
    
    public TimeInterval(double start, double end) {
        this.start = start;
        this.end = end;
    }
    
    public static TimeInterval parse(String s) {
        int         i;
        String[]    defs;
        
        i = s.indexOf("sec");
        if (i >= 0) {
            s = s.substring(0, i);
        }
        defs = s.split("-");
        if (defs.length != 2) {
            throw new RuntimeException("Invalid format: "+ s);
        }
        return new TimeInterval(Double.parseDouble(defs[0]), Double.parseDouble(defs[1]));
    }
    
    public double getStart() {
        return start;
    }

    public double getEnd() {
        return end;
    }
    
    @Override
    public String toString() {
        return start +"-"+ end +" sec";
    }
}

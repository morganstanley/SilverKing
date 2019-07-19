package com.ms.silverking.os.linux.proc;

public class ProcessStatAndOwner {
    private final String        owner;
    private final ProcessStat   stat;
    
    public ProcessStatAndOwner(ProcessStat stat, String owner) {
        this.stat = stat;
        this.owner = owner;
    }
    
    public ProcessStat getStat() {
        return stat;
    }

    public String getOwner() {
        return owner;
    }
    
    @Override
    public String toString() {
        return owner +":"+ stat;
    }
}

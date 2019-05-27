package com.ms.silverking.cloud.argus;

import java.util.Comparator;

import com.ms.silverking.os.linux.proc.ProcessStatAndOwner;

public interface RSSCandidateComparator extends Comparator<ProcessStatAndOwner> {
    public int compare(ProcessStatAndOwner pso1, ProcessStatAndOwner pso2);
}

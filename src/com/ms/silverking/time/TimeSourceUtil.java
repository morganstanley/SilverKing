package com.ms.silverking.time;

import com.google.common.primitives.Ints;

class TimeSourceUtil {
    static int relTimeRemainingAsInt(long deadline, long curTime) {
        return Ints.checkedCast(deadline - curTime);
    }
}

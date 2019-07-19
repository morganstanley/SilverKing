package com.ms.silverking.net.async;


import java.net.ConnectException;

public class UnhealthyConnectionAttemptException extends ConnectException {
    public UnhealthyConnectionAttemptException() {
        super();
    }

    public UnhealthyConnectionAttemptException(String msg) {
        super(msg);
    }
}

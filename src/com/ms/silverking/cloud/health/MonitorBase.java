package com.ms.silverking.cloud.health;

import java.util.ArrayList;
import java.util.List;

public class MonitorBase<T> {
    private List<MonitorListener<T>>   listeners;
    
    public MonitorBase() {
        this.listeners = new ArrayList<>();
    }
    
    public void addListener(MonitorListener<T> listener) {
        listeners.add(listener);
    }
    
    public void sendEvent(T event) {
        for (MonitorListener<T> listener : listeners) {
            listener.sendEvent();
        }
    }
}

package com.ms.silverking.cloud.dht.daemon.storage;

import org.junit.Test;

public class NamespaceMetricsTest {
    @Test
    public void testAggregation() {
        NamespaceMetrics[]    nm;
        NamespaceMetrics      a;
        
        a = new NamespaceMetrics();
        nm = new NamespaceMetrics[3];
        for (int i = 0; i < nm.length; i++) {
            nm[i] = new NamespaceMetrics();
            nm[i].incTotalKeys();
            nm[i].addBytes(2, 1);
            nm[i].addPuts(2, 1, System.currentTimeMillis());
            nm[i].addRetrievals(1, System.currentTimeMillis());
            a = NamespaceMetrics.aggregate(nm[i], a);
        }
        for (String n : NamespaceMetrics.getMetricNames()) {
            System.out.printf("%s\t%s\n", n, a.getMetric(n));
        }
    }
}

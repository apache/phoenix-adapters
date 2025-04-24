package org.apache.phoenix.ddb.rest.metrics;

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;

public class MetricsREST {

    public MetricsRESTSource getSource() {
        return source;
    }

    private MetricsRESTSource source;

    public MetricsREST() {
        source = new MetricsRESTSourceImpl();
    }

    /**
     * @param inc How much to add to requests.
     */
    public void incrementRequests(final int inc) {
        source.incrementRequests(inc);
    }

    /**
     * @param inc How much to add to create table success requests.
     */
    public void incrementCreateTableSuccessRequests(int inc) {
        source.incrementCreateTableSuccessRequests(inc);
    }

    /**
     * @param inc How much to add to create table failure requests.
     */
    public void incrementCreateTableFailedRequests(int inc) {
        source.incrementCreateTableFailedRequests(inc);
    }

}

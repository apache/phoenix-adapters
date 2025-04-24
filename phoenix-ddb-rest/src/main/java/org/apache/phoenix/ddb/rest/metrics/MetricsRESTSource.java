package org.apache.phoenix.ddb.rest.metrics;

import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.hadoop.hbase.metrics.JvmPauseMonitorSource;

/**
 * Interface of the Metrics Source that will export data to Hadoop's Metrics2 system.
 */
public interface MetricsRESTSource extends BaseSource, JvmPauseMonitorSource {

    String METRICS_NAME = "REST";

    String CONTEXT = "rest";

    String JMX_CONTEXT = "REST";

    String METRICS_DESCRIPTION = "Metrics for the Phoenix DynamoDB REST server";

    String REQUEST_KEY = "requests";

    String CREATE_TABLE_SUCCESS_KEY = "createTableSuccess";

    String CREATE_TABLE_FAIL_KEY = "createTableFailure";

    /**
     * Increment the number of requests
     * @param inc Ammount to increment by
     */
    void incrementRequests(int inc);

    /**
     * Increment the number of successful Create Table requests.
     * @param inc Number of successful get requests.
     */
    void incrementCreateTableSuccessRequests(int inc);

    /**
     * Increment the number of failed Create Table requests.
     * @param inc The number of failed Get Requests.
     */
    void incrementCreateTableFailedRequests(int inc);

}

package org.apache.phoenix.ddb.rest.metrics;

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Hadoop Two implementation of a metrics2 source that will export metrics from the Rest server to
 * the hadoop metrics2 subsystem.
 */
@InterfaceAudience.Private
public class MetricsRESTSourceImpl extends BaseSourceImpl implements MetricsRESTSource {

    // rest metrics
    private MutableFastCounter request;
    private MutableFastCounter ctS;
    private MutableFastCounter ctF;

    // pause monitor metrics
    private final MutableFastCounter infoPauseThresholdExceeded;
    private final MutableFastCounter warnPauseThresholdExceeded;
    private final MetricHistogram pausesWithGc;
    private final MetricHistogram pausesWithoutGc;

    public MetricsRESTSourceImpl() {
        this(METRICS_NAME, METRICS_DESCRIPTION, CONTEXT, JMX_CONTEXT);
    }

    public MetricsRESTSourceImpl(String metricsName, String metricsDescription, String metricsContext,
                                 String metricsJmxContext) {
        super(metricsName, metricsDescription, metricsContext, metricsJmxContext);

        // pause monitor metrics
        infoPauseThresholdExceeded =
                getMetricsRegistry().newCounter(INFO_THRESHOLD_COUNT_KEY, INFO_THRESHOLD_COUNT_DESC, 0L);
        warnPauseThresholdExceeded =
                getMetricsRegistry().newCounter(WARN_THRESHOLD_COUNT_KEY, WARN_THRESHOLD_COUNT_DESC, 0L);
        pausesWithGc = getMetricsRegistry().newTimeHistogram(PAUSE_TIME_WITH_GC_KEY);
        pausesWithoutGc = getMetricsRegistry().newTimeHistogram(PAUSE_TIME_WITHOUT_GC_KEY);
    }

    @Override
    public void init() {
        super.init();
        request = getMetricsRegistry().getCounter(REQUEST_KEY, 0L);
        ctS = getMetricsRegistry().getCounter(CREATE_TABLE_SUCCESS_KEY, 0L);
        ctF = getMetricsRegistry().getCounter(CREATE_TABLE_FAIL_KEY, 0L);
    }

    @Override
    public void incrementRequests(int inc) {
        request.incr(inc);
    }

    @Override
    public void incrementCreateTableSuccessRequests(int inc) {
        ctS.incr(inc);
    }

    @Override
    public void incrementCreateTableFailedRequests(int inc) {
        ctF.incr(inc);
    }

    @Override
    public void incInfoThresholdExceeded(int count) {
        infoPauseThresholdExceeded.incr(count);
    }

    @Override
    public void incWarnThresholdExceeded(int count) {
        warnPauseThresholdExceeded.incr(count);
    }

    @Override
    public void updatePauseTimeWithGc(long t) {
        pausesWithGc.add(t);
    }

    @Override
    public void updatePauseTimeWithoutGc(long t) {
        pausesWithoutGc.add(t);
    }
}

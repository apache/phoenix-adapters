package org.apache.phoenix.ddb.rest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.ConnectionCache;
import org.apache.hadoop.hbase.util.JvmPauseMonitor;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.phoenix.ddb.rest.metrics.MetricsREST;
import org.apache.phoenix.ddb.rest.util.Constants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RESTServlet {

    private static final Logger LOG = LoggerFactory.getLogger(RESTServlet.class);

    private static RESTServlet INSTANCE;
    private final Configuration conf;
    private final ConnectionCache connectionCache;
    private final MetricsREST metrics;
    private final UserGroupInformation realUser;
    private final JvmPauseMonitor pauseMonitor;

    UserGroupInformation getRealUser() {
        return realUser;
    }

    /**
     * Returns the RESTServlet singleton instance
     */
    public synchronized static RESTServlet getInstance() {
        assert (INSTANCE != null);
        return INSTANCE;
    }

    /**
     * Returns the ConnectionCache instance
     */
    public ConnectionCache getConnectionCache() {
        return connectionCache;
    }

    /**
     * @param conf         Existing configuration to use in rest servlet
     * @param userProvider the login user provider
     * @return the RESTServlet singleton instance
     */
    public synchronized static RESTServlet getInstance(Configuration conf,
            UserProvider userProvider) throws IOException {
        if (INSTANCE == null) {
            INSTANCE = new RESTServlet(conf, userProvider);
        }
        return INSTANCE;
    }

    public synchronized static void stop() {
        if (INSTANCE != null) {
            INSTANCE.shutdown();
            INSTANCE = null;
        }
    }

    /**
     * Constructor with existing configuration
     *
     * @param conf         existing configuration
     * @param userProvider the login user provider
     */
    RESTServlet(final Configuration conf, final UserProvider userProvider) throws IOException {
        this.realUser = userProvider.getCurrent().getUGI();
        this.conf = conf;

        int cleanInterval = conf.getInt(Constants.CLEANUP_INTERVAL, 10 * 1000);
        int maxIdleTime = conf.getInt(Constants.MAX_IDLETIME, 10 * 60 * 1000);
        connectionCache = new ConnectionCache(conf, userProvider, cleanInterval, maxIdleTime);
        if (supportsProxyuser()) {
            ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
        }

        metrics = new MetricsREST();
        pauseMonitor = new JvmPauseMonitor(conf, metrics.getSource());
        pauseMonitor.start();
    }

    Configuration getConfiguration() {
        return conf;
    }

    MetricsREST getMetrics() {
        return metrics;
    }

    /**
     * Shutdown any services that need to stop
     */
    void shutdown() {
        if (pauseMonitor != null) {
            pauseMonitor.stop();
        }
        if (connectionCache != null) {
            connectionCache.shutdown();
        }
    }

    boolean supportsProxyuser() {
        return conf.getBoolean(Constants.SUPPORT_PROXY_USER, false);
    }

}

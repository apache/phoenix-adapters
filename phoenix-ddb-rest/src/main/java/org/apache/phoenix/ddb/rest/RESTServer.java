/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.ddb.rest;

import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.servlet.DispatcherType;

import org.apache.hbase.thirdparty.com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.HelpFormatter;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Options;
import org.apache.hbase.thirdparty.org.apache.commons.cli.ParseException;
import org.apache.hbase.thirdparty.org.apache.commons.cli.PosixParser;
import org.apache.hbase.thirdparty.org.eclipse.jetty.jmx.MBeanContainer;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.HttpConfiguration;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.HttpConnectionFactory;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.Server;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.ServerConnector;
import org.apache.hbase.thirdparty.org.eclipse.jetty.servlet.FilterHolder;
import org.apache.hbase.thirdparty.org.eclipse.jetty.servlet.ServletContextHandler;
import org.apache.hbase.thirdparty.org.eclipse.jetty.servlet.ServletHolder;
import org.apache.hbase.thirdparty.org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.apache.hbase.thirdparty.org.glassfish.jersey.server.ResourceConfig;
import org.apache.hbase.thirdparty.org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.http.HttpServerUtil;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.phoenix.ddb.ConnectionUtil;
import org.apache.phoenix.ddb.rest.auth.AccessKeyAuthFilter;
import org.apache.phoenix.ddb.rest.auth.CredentialStore;
import org.apache.phoenix.ddb.rest.util.Constants;
import org.apache.phoenix.ddb.TableOptionsConfig;
import org.apache.phoenix.ddb.service.utils.SegmentScanUtil;
import org.apache.phoenix.ddb.utils.IndexBuildingActivator;
import org.apache.phoenix.ddb.utils.PhoenixUtils;

/**
 * Main class for launching REST gateway as a servlet hosted by Jetty.
 */
public class RESTServer {

    private static final Logger LOG = LoggerFactory.getLogger(RESTServer.class);

    private final Configuration conf;
    private final UserProvider userProvider;
    private Server server;
    private ServerName serverName;
    private ExecutorService indexBuildingActivator;
    private static final String ZK_QUORUM = "zkquorum";
    private static final String PORT = "port";
    private static final String START = "start";
    private static final String STOP = "stop";
    private static final String SERVER_NOT_RUNNING_ERR_MSG = "Server is not running";

    public RESTServer(Configuration conf) {
        this.conf = conf;
        this.userProvider = UserProvider.instantiate(conf);
        PhoenixUtils.registerDriver();
    }

    private static void printUsageAndExit(Options options, int exitCode) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("phoenix ddb rest start", "", options,
                "\nTo run the REST server as a daemon, execute "
                        + "phoenix-ddb.sh start|stop rest [-i <port>] [-p <port>] [-z <zk_quorum_for_phoenix_connection>]\n",
                true);
        System.exit(exitCode);
    }

    private static void parseCommandLine(String[] args, Configuration conf) {
        Options options = new Options();
        options.addOption("p", PORT, true,
                "Port to bind to [default: " + Constants.DEFAULT_LISTEN_PORT + "]");
        options.addOption("z", ZK_QUORUM, true, "ZK Quorum to be used for Phoenix Connection");

        CommandLine commandLine = null;
        try {
            commandLine = new PosixParser().parse(options, args);
        } catch (ParseException e) {
            LOG.error("Could not parse: ", e);
            printUsageAndExit(options, -1);
        }

        // check for user-defined port setting, if so override the conf
        if (commandLine != null && commandLine.hasOption(PORT)) {
            String val = commandLine.getOptionValue(PORT);
            conf.setInt(Constants.PHOENIX_DDB_REST_PORT, Integer.parseInt(val));
            LOG.debug("port set to {}", val);
        }

        // check for user-defined zookeeper quorum setting, if so override the conf
        if (commandLine != null && commandLine.hasOption(ZK_QUORUM)) {
            String val = commandLine.getOptionValue(ZK_QUORUM);
            conf.set(Constants.PHOENIX_DDB_ZK_QUORUM, val);
            LOG.debug("ZK Quorum set to {}", val);
        }

        List<String> remainingArgs =
                commandLine != null ? commandLine.getArgList() : new ArrayList<>();
        if (remainingArgs.size() != 1) {
            printUsageAndExit(options, 1);
        }

        String command = remainingArgs.get(0);
        if (START.equals(command)) {
            // continue and start container
        } else if (STOP.equals(command)) {
            System.exit(1);
        } else {
            printUsageAndExit(options, 1);
        }
    }

    /**
     * Runs the REST server.
     */
    public synchronized void run() throws Exception {
        Class<? extends ServletContainer> containerClass = ServletContainer.class;

        if (conf.get(Constants.PHOENIX_DDB_ZK_QUORUM) == null) {
            String clientZkQuorumServers = ZKConfig.getClientZKQuorumServersString(conf);
            String quorum = clientZkQuorumServers != null ?
                    clientZkQuorumServers :
                    ZKConfig.getZKQuorumServersString(conf);
            quorum = quorum.replaceAll(":", "\\\\:");
            conf.set(Constants.PHOENIX_DDB_ZK_QUORUM, quorum);
        }

        try {
            String url = PhoenixUtils.URL_ZK_PREFIX + conf.get(Constants.PHOENIX_DDB_ZK_QUORUM);
            validateConnection(url);
            indexBuildingActivator = startIndexBuildingActivator(url);
            TableOptionsConfig.initialize();
            SegmentScanUtil.createSegmentScanMetadataTable(url);
        } catch (Exception e) {
            LOG.error("Failed to validate connection, shutting down REST Server...", e);
            throw e;
        }

        RESTServlet servlet = RESTServlet.getInstance(conf, userProvider);

        // set up the Jersey servlet container for Jetty
        ResourceConfig application = new ResourceConfig().packages("org.apache.phoenix.ddb.rest")
                .register(JacksonJaxbJsonProvider.class);
        // Using our custom ServletContainer is tremendously important. This is what makes sure the
        // UGI.doAs() is done for the remoteUser, and calls are not made as the REST server itself.
        ServletContainer servletContainer =
                ReflectionUtils.newInstance(containerClass, application);
        ServletHolder sh = new ServletHolder(servletContainer);

        // ServletHolder for JMX metrics
        ServletHolder jmxSh = new ServletHolder("jmx", JMXJsonServlet.class);

        // Set the default max thread number to 100 to limit
        // the number of concurrent requests so that REST server doesn't OOM easily.
        // Jetty set the default max thread number to 250, if we don't set it.
        //
        // Our default min thread number 2 is the same as that used by Jetty.
        int maxThreads =
                servlet.getConfiguration().getInt(Constants.REST_THREAD_POOL_THREADS_MAX, 125);
        int minThreads =
                servlet.getConfiguration().getInt(Constants.REST_THREAD_POOL_THREADS_MIN, 2);
        // Use the default queue (unbounded with Jetty 9.3) if the queue size is negative, otherwise use
        // bounded {@link ArrayBlockingQueue} with the given size
        int queueSize =
                servlet.getConfiguration().getInt(Constants.REST_THREAD_POOL_TASK_QUEUE_SIZE, -1);
        int idleTimeout = servlet.getConfiguration()
                .getInt(Constants.REST_THREAD_POOL_THREAD_IDLE_TIMEOUT, 60000);
        QueuedThreadPool threadPool = queueSize > 0 ?
                new QueuedThreadPool(maxThreads, minThreads, idleTimeout,
                        new ArrayBlockingQueue<>(queueSize)) :
                new QueuedThreadPool(maxThreads, minThreads, idleTimeout);

        this.server = new Server(threadPool);

        // Setup JMX
        MBeanContainer mbContainer = new MBeanContainer(ManagementFactory.getPlatformMBeanServer());
        server.addEventListener(mbContainer);
        server.addBean(mbContainer);

        String host =
                servlet.getConfiguration().get("phoenix.ddb.rest.host", Constants.DEFAULT_HOST);
        int servicePort = servlet.getConfiguration()
                .getInt(Constants.PHOENIX_DDB_REST_PORT, Constants.DEFAULT_LISTEN_PORT);
        int httpHeaderCacheSize = servlet.getConfiguration()
                .getInt(Constants.HTTP_HEADER_CACHE_SIZE, Constants.DEFAULT_HTTP_HEADER_CACHE_SIZE);

        HttpConfiguration httpConfig = new HttpConfiguration();
        httpConfig.setSecureScheme("https");
        httpConfig.setSecurePort(servicePort);
        httpConfig.setHeaderCacheSize(httpHeaderCacheSize);
        httpConfig.setRequestHeaderSize(Constants.DEFAULT_HTTP_MAX_HEADER_SIZE);
        httpConfig.setResponseHeaderSize(Constants.DEFAULT_HTTP_MAX_HEADER_SIZE);
        httpConfig.setSendServerVersion(false);

        ServerConnector serverConnector;
        serverConnector = new ServerConnector(server, new HttpConnectionFactory(httpConfig));

        int acceptQueueSize =
                servlet.getConfiguration().getInt(Constants.REST_CONNECTOR_ACCEPT_QUEUE_SIZE, -1);
        if (acceptQueueSize >= 0) {
            serverConnector.setAcceptQueueSize(acceptQueueSize);
        }

        int httpIdleTimeout = servlet.getConfiguration()
                .getInt(Constants.REST_HTTP_IDLE_TIMEOUT, Constants.DEFAULT_HTTP_IDLE_TIMEOUT);
        serverConnector.setIdleTimeout(httpIdleTimeout);

        serverConnector.setPort(servicePort);
        serverConnector.setHost(host);

        server.addConnector(serverConnector);
        server.setStopAtShutdown(true);

        // set up context
        ServletContextHandler ctxHandler =
                new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);
        ctxHandler.addServlet(sh, Constants.PATH_SPEC_ANY);
        ctxHandler.addServlet(jmxSh, "/jmx");

        String credentialStoreClass =
                servlet.getConfiguration().get(Constants.AUTH_CREDENTIAL_STORE_CLASS);
        if (credentialStoreClass != null && !credentialStoreClass.trim().isEmpty()) {
            try {
                Class<?> storeClass = Class.forName(credentialStoreClass);
                Object credentialStore = storeClass.getDeclaredConstructor().newInstance();
                AccessKeyAuthFilter authFilter =
                        new AccessKeyAuthFilter((CredentialStore) credentialStore);
                ctxHandler.addFilter(new FilterHolder(authFilter), "/*",
                        EnumSet.of(DispatcherType.REQUEST));
                LOG.info("Authentication filter enabled with credential store: {}",
                        credentialStoreClass);
            } catch (Exception e) {
                LOG.error("Failed to initialize authentication filter with credential store: {}",
                        credentialStoreClass, e);
                throw new RuntimeException("Failed to initialize authentication", e);
            }
        }

        HttpServerUtil.constrainHttpMethods(ctxHandler, servlet.getConfiguration()
                .getBoolean(Constants.REST_HTTP_ALLOW_OPTIONS_METHOD,
                        Constants.REST_HTTP_ALLOW_OPTIONS_METHOD_DEFAULT));

        final long startCode = EnvironmentEdgeManager.currentTime();
        this.serverName = ServerName.valueOf(getHostName(conf), servicePort, startCode);
        server.start();
    }

    private void validateConnection(String jdbcUrl) throws SQLException {
        LOG.info("Try connecting to SYSTEM.CATALOG using JDBC connection url: {}", jdbcUrl);
        try (Connection connection = ConnectionUtil.getConnection(jdbcUrl)) {
            ResultSet resultSet = connection.createStatement()
                    .executeQuery("SELECT * FROM SYSTEM.CATALOG LIMIT 1");
            resultSet.next();
        }
    }

    private ExecutorService startIndexBuildingActivator(String jdbcUrl) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            try (Connection connection = ConnectionUtil.getConnection(jdbcUrl)) {
                IndexBuildingActivator.activateIndexesForBuilding(connection, 1800000);
            } catch (SQLException e) {
                LOG.info("Error while running IndexBuildingActivator. ", e);
            }
        }, 0, 15, TimeUnit.MINUTES);
        LOG.info("Scheduled IndexBuildingActivator.");
        return scheduler;
    }

    private static String getHostName(Configuration conf) throws UnknownHostException {
        return Strings.domainNamePointerToHostName(
                DNS.getDefaultHost(conf.get(Constants.REST_DNS_INTERFACE, "default"),
                        conf.get(Constants.REST_DNS_NAMESERVER, "default")));
    }

    public synchronized void join() throws Exception {
        if (server == null) {
            throw new IllegalStateException(SERVER_NOT_RUNNING_ERR_MSG);
        }
        server.join();
    }

    public synchronized void stop() throws Exception {
        if (server == null) {
            throw new IllegalStateException(SERVER_NOT_RUNNING_ERR_MSG);
        }
        if (indexBuildingActivator != null) {
            indexBuildingActivator.shutdown();
        }
        server.stop();
        server = null;
        RESTServlet.stop();
    }

    public synchronized int getPort() {
        if (server == null) {
            throw new IllegalStateException(SERVER_NOT_RUNNING_ERR_MSG);
        }
        return ((ServerConnector) server.getConnectors()[0]).getLocalPort();
    }

    public String getServerAddress() {
        return serverName.getAddress().toString();
    }

    public Configuration getConf() {
        return conf;
    }

    /**
     * The main method for the Phoenix DynamoDB rest server.
     *
     * @param args command-line arguments
     * @throws Exception exception
     */
    public static void main(String[] args) throws Exception {
        LOG.info("***** STARTING service '" + RESTServer.class.getSimpleName() + "' *****");
        final Configuration conf = HBaseConfiguration.create();

        String zkQuorum = System.getenv("ZOO_KEEPER_QUORUM");
        if (zkQuorum != null) {
            conf.set(Constants.PHOENIX_DDB_ZK_QUORUM, zkQuorum);
        }

        parseCommandLine(args, conf);
        RESTServer server = new RESTServer(conf);

        try {
            server.run();
            server.join();
        } catch (Exception e) {
            LOG.error(HBaseMarkers.FATAL, "Failed to start server", e);
            System.exit(1);
        }

        LOG.info("***** STOPPING service '" + RESTServer.class.getSimpleName() + "' *****");
    }
}

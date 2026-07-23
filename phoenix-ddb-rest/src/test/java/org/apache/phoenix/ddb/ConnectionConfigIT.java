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
package org.apache.phoenix.ddb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.ConnectionUtil;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class ConnectionConfigIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionConfigIT.class);

    private static String url;
    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;

    @Rule
    public final TestName testName = new TestName();

    @BeforeClass
    public static void initialize() throws Exception {
        tmpDir = System.getProperty("java.io.tmpdir");
        Configuration conf = TestUtils.getConfigForMiniCluster();
        utility = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);
        utility.startMiniCluster();
        String zkQuorum = "127.0.0.1:" + utility.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
        TestUtils.awaitPhoenixReady(url);

        restServer = new RESTServer(utility.getConfiguration());
        restServer.run();
        LOGGER.info("started {} on port {}", restServer.getClass().getName(), restServer.getPort());
    }

    @AfterClass
    public static void teardown() throws Exception {
        if (restServer != null) {
            restServer.stop();
        }
        ServerUtil.ConnectionFactory.shutdown();
        try {
            DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
        } finally {
            if (utility != null) {
                utility.shutdownMiniCluster();
            }
            ServerMetadataCacheTestImpl.resetCache();
        }
        System.setProperty("java.io.tmpdir", tmpDir);
    }

    @Test
    public void testConnectionConfigs() throws Exception {
        Properties expectedProps = ConnectionUtil.getMutableProps();
        try (Connection connection = DriverManager.getConnection(url)) {
            Configuration actualConfig =
                    connection.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()
                            .getConfiguration();
            for (String config : expectedProps.stringPropertyNames()) {
                Assert.assertEquals(expectedProps.getProperty(config), actualConfig.get(config));
            }
        }
        try (Connection connection = DriverManager.getConnection(url, expectedProps)) {
            Configuration actualConfig =
                    connection.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()
                            .getConfiguration();
            for (String config : expectedProps.stringPropertyNames()) {
                Assert.assertEquals(expectedProps.getProperty(config), actualConfig.get(config));
            }
        }
    }

}
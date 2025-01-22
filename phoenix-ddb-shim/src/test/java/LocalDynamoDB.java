/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.util.Optional;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import org.junit.Assert;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * Wrapper for a local DynamoDb server used in testing. Each instance of this class will find a
 * new port to run on, so multiple instances can be safely run simultaneously.
 * Each instance of this service uses memory as a storage medium and is thus completely ephemeral;
 * no data will be persisted between stops and starts.
 */
class LocalDynamoDB {
    private DynamoDBProxyServer server;
    private int port;

    /**
     * Start the local DynamoDb service and run in background
     */
    void start() {
        port = getFreePort();
        String portString = Integer.toString(port);

        try {
            server = createServer(portString);
            server.start();
        } catch (Exception e) {
            throw propagate(e);
        }
    }

    /**
     * Create a standard AWS v2 SDK client pointing to the local DynamoDb instance
     *
     * @return A DynamoDbClient pointing to the local DynamoDb instance
     */
    DynamoDbClient createClient() {
        String endpoint = String.format("http://localhost:%d", port);
        return DynamoDbClient.builder()
                .endpointOverride(URI.create(endpoint))
                // The region is meaningless for local DynamoDb but required for client builder validation
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("dummykey", "dummysecret")))
                .overrideConfiguration(
                        o -> o.addExecutionInterceptor(new VerifyUserAgentInterceptor()))
                .build();
    }

    /**
     * Create a standard AWS v1 SDK client pointing to the local DynamoDb instance
     *
     * @return A DynamoDbClient pointing to the local DynamoDb instance
     */
    AmazonDynamoDB createV1Client() {
        String endpoint = String.format("http://localhost:%d", port);
        return AmazonDynamoDBClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials("dummykey", "dummysecret")))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        endpoint, "us-west-2"))
                .build();
    }

    DynamoDbAsyncClient createAsyncClient() {
        String endpoint = String.format("http://localhost:%d", port);
        return DynamoDbAsyncClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("dummykey", "dummysecret")))
                .overrideConfiguration(
                        o -> o.addExecutionInterceptor(new VerifyUserAgentInterceptor()))
                .build();
    }

    AmazonDynamoDBStreams createV1StreamsClient() {
        String endpoint = String.format("http://localhost:%d", port);
        return AmazonDynamoDBStreamsClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials("dummykey", "dummysecret")))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        endpoint, "us-west-2"))
                .build();
    }

    /**
     * Stops the local DynamoDb service and frees up resources it is using.
     */
    void stop() {
        try {
            server.stop();
        } catch (Exception e) {
            throw propagate(e);
        }
    }

    private DynamoDBProxyServer createServer(String portString) throws Exception {
        return ServerRunner.createServerFromCommandLineArgs(
                new String[]{
                        "-inMemory",
                        "-port", portString
                });
    }

    private int getFreePort() {
        try {
            ServerSocket socket = new ServerSocket(0);
            int port = socket.getLocalPort();
            socket.close();
            return port;
        } catch (IOException ioe) {
            throw propagate(ioe);
        }
    }

    private static RuntimeException propagate(Exception e) {
        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        }
        throw new RuntimeException(e);
    }

    private static class VerifyUserAgentInterceptor implements ExecutionInterceptor {

        @Override
        public void beforeTransmission(Context.BeforeTransmission context,
                                       ExecutionAttributes executionAttributes) {
            Optional<String> headers = context.httpRequest().firstMatchingHeader("User-agent");
            Assert.assertTrue(headers.isPresent());
            Assert.assertTrue(headers.get().contains("hll/ddb-enh"));
        }
    }

}

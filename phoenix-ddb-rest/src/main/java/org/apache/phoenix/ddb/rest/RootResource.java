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

import java.util.HashMap;
import java.util.Map;

import org.apache.hbase.thirdparty.javax.ws.rs.HeaderParam;
import org.apache.hbase.thirdparty.javax.ws.rs.POST;
import org.apache.hbase.thirdparty.javax.ws.rs.Path;
import org.apache.hbase.thirdparty.javax.ws.rs.Produces;

import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.phoenix.ddb.service.BatchGetItemService;
import org.apache.phoenix.ddb.service.BatchWriteItemService;
import org.apache.phoenix.ddb.service.DescribeStreamService;
import org.apache.phoenix.ddb.service.GetRecordsService;
import org.apache.phoenix.ddb.service.GetShardIteratorService;
import org.apache.phoenix.ddb.service.ListStreamsService;

import org.apache.phoenix.ddb.service.ListTablesService;
import org.apache.phoenix.ddb.service.UpdateTableService;
import org.apache.phoenix.ddb.service.utils.ApiMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.javax.ws.rs.core.CacheControl;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Context;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Response;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Response.ResponseBuilder;
import org.apache.hbase.thirdparty.javax.ws.rs.core.UriInfo;

import javax.ws.rs.Consumes;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.ddb.rest.util.Constants;
import org.apache.phoenix.ddb.service.CreateTableService;
import org.apache.phoenix.ddb.service.DeleteItemService;
import org.apache.phoenix.ddb.service.DeleteTableService;
import org.apache.phoenix.ddb.service.GetItemService;
import org.apache.phoenix.ddb.service.PutItemService;
import org.apache.phoenix.ddb.service.QueryService;
import org.apache.phoenix.ddb.service.ScanService;
import org.apache.phoenix.ddb.service.TTLService;
import org.apache.phoenix.ddb.service.UpdateItemService;
import org.apache.phoenix.ddb.service.utils.TableDescriptorUtils;
import org.apache.phoenix.ddb.service.utils.exceptions.ConditionCheckFailedException;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.schema.TableNotFoundException;

@Path("/")
public class RootResource {

    private static final Logger LOG = LoggerFactory.getLogger(RootResource.class);

    static CacheControl cacheControl;

    static {
        cacheControl = new CacheControl();
        cacheControl.setNoCache(true);
        cacheControl.setNoTransform(false);
    }

    private final RESTServlet servlet;
    private final String jdbcConnectionUrl;

    public RootResource() {
        super();
        servlet = RESTServlet.getInstance();
        Configuration conf = servlet.getConfiguration();
        jdbcConnectionUrl = PhoenixUtils.URL_ZK_PREFIX + conf.get(Constants.PHOENIX_DDB_ZK_QUORUM);
        LOG.info("JDBC connection url: {}", jdbcConnectionUrl);
    }

    @POST
    @Consumes({Constants.APPLICATION_AMZ_JSON, MediaType.APPLICATION_JSON})
    @Produces({Constants.APPLICATION_AMZ_JSON, MediaType.APPLICATION_JSON})
    public Response get(final @Context UriInfo uriInfo,
            final @HeaderParam("Content-Type") String contentType,
            final @HeaderParam("X-Amz-Target") String api,
            final Map<String, Object> request) {
        long startTime = EnvironmentEdgeManager.currentTime();
        try {
            LOG.info("Content Type: {}, api: {}, Request: {}", contentType, api, request);
            servlet.getMetrics().incrementRequests(1);
            final Map<String, Object> responseObject;

            switch (api) {
                case "DynamoDB_20120810.CreateTable": {
                    responseObject = CreateTableService.createTable(request, jdbcConnectionUrl);
                    servlet.getMetrics().createTableSuccessTime(
                            EnvironmentEdgeManager.currentTime() - startTime);
                    break;
                }
                case "DynamoDB_20120810.DeleteTable": {
                    responseObject = DeleteTableService.deleteTable(request, jdbcConnectionUrl);
                    break;
                }
                case "DynamoDB_20120810.DescribeTable": {
                    responseObject = TableDescriptorUtils.getTableDescription(
                            (String) request.get(ApiMetadata.TABLE_NAME), jdbcConnectionUrl, "Table");
                    break;
                }
                case "DynamoDB_20120810.ListTables": {
                    responseObject = ListTablesService.listTables(request, jdbcConnectionUrl);
                    break;
                }
                case "DynamoDB_20120810.UpdateTable": {
                    responseObject = UpdateTableService.updateTable(request, jdbcConnectionUrl);
                    break;
                }
                case "DynamoDB_20120810.PutItem": {
                    try {
                        responseObject = PutItemService.putItem(request, jdbcConnectionUrl);
                    } catch (ConditionCheckFailedException e) {
                        return getResponseForConditionCheckFailure(e);
                    }
                    break;
                }
                case "DynamoDB_20120810.UpdateItem": {
                    try {
                        responseObject = UpdateItemService.updateItem(request, jdbcConnectionUrl);
                    } catch (ConditionCheckFailedException e) {
                        return getResponseForConditionCheckFailure(e);
                    }
                    break;
                }
                case "DynamoDB_20120810.DeleteItem": {
                    try {
                        responseObject = DeleteItemService.deleteItem(request, jdbcConnectionUrl);
                    } catch (ConditionCheckFailedException e) {
                        return getResponseForConditionCheckFailure(e);
                    }
                    break;
                }
                case "DynamoDB_20120810.GetItem": {
                    responseObject = GetItemService.getItem(request, jdbcConnectionUrl);
                    break;
                }
                case "DynamoDB_20120810.BatchGetItem": {
                    responseObject = BatchGetItemService.batchGetItem(request, jdbcConnectionUrl);
                    break;
                }
                case "DynamoDB_20120810.BatchWriteItem": {
                    responseObject =
                            BatchWriteItemService.batchWriteItem(request, jdbcConnectionUrl);
                    break;
                }
                case "DynamoDB_20120810.Query": {
                    responseObject = QueryService.query(request, jdbcConnectionUrl);
                    break;
                }
                case "DynamoDB_20120810.Scan": {
                    responseObject = ScanService.scan(request, jdbcConnectionUrl);
                    break;
                }
                case "DynamoDB_20120810.UpdateTimeToLive": {
                    responseObject = TTLService.updateTimeToLive(request, jdbcConnectionUrl);
                    break;
                }
                case "DynamoDB_20120810.DescribeTimeToLive": {
                    responseObject = TTLService.describeTimeToLive(request, jdbcConnectionUrl);
                    break;
                }
                case "DynamoDBStreams_20120810.ListStreams": {
                    responseObject = ListStreamsService.listStreams(request, jdbcConnectionUrl);
                    break;
                }
                case "DynamoDBStreams_20120810.DescribeStream": {
                    responseObject =
                            DescribeStreamService.describeStream(request, jdbcConnectionUrl);
                    break;
                }
                case "DynamoDBStreams_20120810.GetShardIterator": {
                    responseObject =
                            GetShardIteratorService.getShardIterator(request, jdbcConnectionUrl);
                    break;
                }
                case "DynamoDBStreams_20120810.GetRecords": {
                    responseObject = GetRecordsService.getRecords(request, jdbcConnectionUrl);
                    break;
                }
                default: {
                    throw new IllegalArgumentException("Unknown API: " + api);
                }
            }

            ResponseBuilder response = Response.ok(responseObject);
            response.cacheControl(cacheControl);
            return response.build();
        } catch (Exception e) {
            LOG.error("Error... Content Type: {}, api: {}, Request: {} ", contentType, api,
                    request, e);
            // TODO : metrics for error response
            switch (api) {
                case "DynamoDB_20120810.CreateTable": {
                    servlet.getMetrics().createTableFailureTime(
                            EnvironmentEdgeManager.currentTime() - startTime);
                    break;
                }
                case "DynamoDB_20120810.DeleteTable": {
                    break;
                }
                case "DynamoDB_20120810.DescribeTable": {
                    break;
                }
                case "DynamoDB_20120810.PutItem": {
                    break;
                }
                case "DynamoDB_20120810.DeleteItem": {
                    break;
                }
                case "DynamoDB_20120810.GetItem": {
                    break;
                }
                case "DynamoDB_20120810.Query": {
                    break;
                }
                case "DynamoDB_20120810.Scan": {
                    break;
                }
                default: {
                }
            }
            if (e.getCause() != null && e.getCause() instanceof TableNotFoundException) {
                return getResponseForTableNotFoundFailure();
            }
            throw e;
        }
    }

    private static Response getResponseForTableNotFoundFailure() {
        Map<String, Object> respObj = new HashMap<>();
        respObj.put("__type",
                "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException");
        respObj.put("Message", "Cannot do operations on a non-existent table");
        return Response.status(Response.Status.BAD_REQUEST).entity(respObj).build();
    }

    private static Response getResponseForConditionCheckFailure(ConditionCheckFailedException e) {
        Map<String, Object> item = e.getItem();
        Map<String, Object> respObj = new HashMap<>();
        respObj.put("__type",
                "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException");
        respObj.put("Message", "The conditional request failed");
        if (item != null) {
            respObj.put("Item", item);
        }
        return Response.status(Response.Status.BAD_REQUEST).entity(respObj).build();
    }

}

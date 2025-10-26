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
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;

import org.apache.hbase.thirdparty.javax.ws.rs.HeaderParam;
import org.apache.hbase.thirdparty.javax.ws.rs.POST;
import org.apache.hbase.thirdparty.javax.ws.rs.Path;
import org.apache.hbase.thirdparty.javax.ws.rs.Produces;
import org.apache.hbase.thirdparty.javax.ws.rs.core.CacheControl;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Context;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Response;
import org.apache.hbase.thirdparty.javax.ws.rs.core.Response.ResponseBuilder;
import org.apache.hbase.thirdparty.javax.ws.rs.core.UriInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.phoenix.ddb.rest.metrics.ApiOperation;
import org.apache.phoenix.ddb.rest.util.Constants;
import org.apache.phoenix.ddb.service.BatchGetItemService;
import org.apache.phoenix.ddb.service.BatchWriteItemService;
import org.apache.phoenix.ddb.service.CreateTableService;
import org.apache.phoenix.ddb.service.DeleteItemService;
import org.apache.phoenix.ddb.service.DeleteTableService;
import org.apache.phoenix.ddb.service.DescribeContinuousBackupsService;
import org.apache.phoenix.ddb.service.DescribeStreamService;
import org.apache.phoenix.ddb.service.GetItemService;
import org.apache.phoenix.ddb.service.GetRecordsService;
import org.apache.phoenix.ddb.service.GetShardIteratorService;
import org.apache.phoenix.ddb.service.ListStreamsService;
import org.apache.phoenix.ddb.service.ListTablesService;
import org.apache.phoenix.ddb.service.PutItemService;
import org.apache.phoenix.ddb.service.QueryService;
import org.apache.phoenix.ddb.service.ScanService;
import org.apache.phoenix.ddb.service.TTLService;
import org.apache.phoenix.ddb.service.UpdateItemService;
import org.apache.phoenix.ddb.service.UpdateTableService;
import org.apache.phoenix.ddb.service.exceptions.ConditionCheckFailedException;
import org.apache.phoenix.ddb.service.exceptions.ResourceInUseException;
import org.apache.phoenix.ddb.service.exceptions.ValidationException;
import org.apache.phoenix.ddb.service.utils.TableDescriptorUtils;
import org.apache.phoenix.ddb.utils.ApiMetadata;
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
    }

    @POST
    @Consumes({Constants.APPLICATION_AMZ_JSON, MediaType.APPLICATION_JSON})
    @Produces({Constants.APPLICATION_AMZ_JSON, MediaType.APPLICATION_JSON})
    public Response get(final @Context UriInfo uriInfo,
            final @Context HttpServletRequest httpRequest,
            final @HeaderParam("Content-Type") String contentType,
            final @HeaderParam("X-Amz-Target") String api, final Map<String, Object> request) {
        long startTime = EnvironmentEdgeManager.currentTime();
        try {
            String userName = (String) httpRequest.getAttribute("userName");
            String accessKeyId = (String) httpRequest.getAttribute("accessKeyId");

            if (userName == null || accessKeyId == null) {
                LOG.trace("Content Type: {}, api: {}, Request: {}", contentType, api, request);
            } else {
                LOG.trace("Content Type: {}, api: {}, Request: {}, User: {}, AccessKey: {}",
                        contentType, api, request, userName, accessKeyId);
            }
            servlet.getMetrics().incrementRequests(1);

            if (!api.contains(".")) {
                return getResponseForInvalidApiName(api);
            }

            final Map<String, Object> responseObject;
            String operation = getOperationName(api);
            ApiOperation apiOperation = ApiOperation.fromApiName(operation);

            switch (operation) {
                case ApiMetadata.CREATE_TABLE: {
                    responseObject = CreateTableService.createTable(request, jdbcConnectionUrl);
                    break;
                }
                case ApiMetadata.DELETE_TABLE: {
                    responseObject = DeleteTableService.deleteTable(request, jdbcConnectionUrl);
                    break;
                }
                case ApiMetadata.DESCRIBE_TABLE: {
                    responseObject = TableDescriptorUtils.getTableDescription(
                            (String) request.get(ApiMetadata.TABLE_NAME), jdbcConnectionUrl,
                            "Table");
                    break;
                }
                case ApiMetadata.DESCRIBE_CONTINUOUS_BACKUPS: {
                    responseObject =
                            DescribeContinuousBackupsService.describeContinuousBackups(request,
                                    jdbcConnectionUrl);
                    break;
                }
                case ApiMetadata.LIST_TABLES: {
                    responseObject = ListTablesService.listTables(request, jdbcConnectionUrl);
                    break;
                }
                case ApiMetadata.UPDATE_TABLE: {
                    responseObject = UpdateTableService.updateTable(request, jdbcConnectionUrl);
                    break;
                }
                case ApiMetadata.PUT_ITEM: {
                    try {
                        responseObject = PutItemService.putItem(request, jdbcConnectionUrl);
                    } catch (ConditionCheckFailedException e) {
                        return getResponseForConditionCheckFailure(e);
                    }
                    break;
                }
                case ApiMetadata.UPDATE_ITEM: {
                    try {
                        responseObject = UpdateItemService.updateItem(request, jdbcConnectionUrl);
                    } catch (ConditionCheckFailedException e) {
                        return getResponseForConditionCheckFailure(e);
                    }
                    break;
                }
                case ApiMetadata.DELETE_ITEM: {
                    try {
                        responseObject = DeleteItemService.deleteItem(request, jdbcConnectionUrl);
                    } catch (ConditionCheckFailedException e) {
                        return getResponseForConditionCheckFailure(e);
                    }
                    break;
                }
                case ApiMetadata.GET_ITEM: {
                    responseObject = GetItemService.getItem(request, jdbcConnectionUrl);
                    break;
                }
                case ApiMetadata.BATCH_GET_ITEM: {
                    responseObject = BatchGetItemService.batchGetItem(request, jdbcConnectionUrl);
                    break;
                }
                case ApiMetadata.BATCH_WRITE_ITEM: {
                    responseObject =
                            BatchWriteItemService.batchWriteItem(request, jdbcConnectionUrl);
                    break;
                }
                case ApiMetadata.QUERY: {
                    responseObject = QueryService.query(request, jdbcConnectionUrl);
                    break;
                }
                case ApiMetadata.SCAN: {
                    responseObject = ScanService.scan(request, jdbcConnectionUrl);
                    break;
                }
                case ApiMetadata.UPDATE_TIME_TO_LIVE: {
                    responseObject = TTLService.updateTimeToLive(request, jdbcConnectionUrl);
                    break;
                }
                case ApiMetadata.DESCRIBE_TIME_TO_LIVE: {
                    responseObject = TTLService.describeTimeToLive(request, jdbcConnectionUrl);
                    break;
                }
                case ApiMetadata.LIST_STREAMS: {
                    responseObject = ListStreamsService.listStreams(request, jdbcConnectionUrl);
                    break;
                }
                case ApiMetadata.DESCRIBE_STREAM: {
                    responseObject =
                            DescribeStreamService.describeStream(request, jdbcConnectionUrl);
                    break;
                }
                case ApiMetadata.GET_SHARD_ITERATOR: {
                    responseObject =
                            GetShardIteratorService.getShardIterator(request, jdbcConnectionUrl);
                    break;
                }
                case ApiMetadata.GET_RECORDS: {
                    responseObject = GetRecordsService.getRecords(request, jdbcConnectionUrl);
                    break;
                }
                default: {
                    throw new IllegalArgumentException("Unknown API: " + api);
                }
            }

            servlet.getMetrics().recordSuccessTime(apiOperation,
                    EnvironmentEdgeManager.currentTime() - startTime);

            ResponseBuilder response = Response.ok(responseObject);
            response.cacheControl(cacheControl);
            return response.build();
        } catch (ValidationException e) {
            return getResponseForValidationException(e);
        } catch (ResourceInUseException e) {
            return getResponseForResourceInUseException(e);
        } catch (Exception e) {
            if (!isTableNotFoundError(e)) {
                LOG.error("Error... Content Type: {}, api: {}, Request: {} ", contentType, api,
                        request, e);
            } else {
                LOG.trace("Table not found... Content Type: {}, api: {}, Request: {}, Error: {} ",
                        contentType, api, request, e.getCause().getMessage());
            }
            // TODO : metrics for error response
            String operation = getOperationName(api);
            long elapsedTime = EnvironmentEdgeManager.currentTime() - startTime;
            try {
                ApiOperation apiOperation = ApiOperation.fromApiName(operation);
                servlet.getMetrics().recordFailureTime(apiOperation, elapsedTime);
            } catch (IllegalArgumentException iae) {
                // Unknown API operation, ignore metrics recording
            }
            if (isTableNotFoundError(e)) {
                return getResponseForTableNotFoundFailure();
            }
            throw e;
        }
    }

    private static Response getResponseForInvalidApiName(String api) {
        Map<String, Object> respObj = new HashMap<>();
        respObj.put(ApiMetadata.EXCEPTION_TYPE, ApiMetadata.VALIDATION_EXCEPTION);
        respObj.put(ApiMetadata.EXCEPTION_MESSAGE,
                "Invalid API format. Expected format: <Service>.<Operation>, got: " + api);
        return Response.status(Response.Status.BAD_REQUEST).entity(respObj).build();
    }

    private static boolean isTableNotFoundError(Exception e) {
        return e.getCause() != null && e.getCause() instanceof TableNotFoundException;
    }

    private static Response getResponseForTableNotFoundFailure() {
        Map<String, Object> respObj = new HashMap<>();
        respObj.put(ApiMetadata.EXCEPTION_TYPE, ApiMetadata.RESOURCE_NOT_FOUND_EXCEPTION);
        respObj.put(ApiMetadata.EXCEPTION_MESSAGE, "Cannot do operations on a non-existent table");
        return Response.status(Response.Status.BAD_REQUEST).entity(respObj).build();
    }

    private static Response getResponseForConditionCheckFailure(ConditionCheckFailedException e) {
        Map<String, Object> item = e.getItem();
        Map<String, Object> respObj = new HashMap<>();
        respObj.put(ApiMetadata.EXCEPTION_TYPE, ApiMetadata.CONDITIONAL_CHECK_FAILED_EXCEPTION);
        respObj.put(ApiMetadata.EXCEPTION_MESSAGE, "The conditional request failed");
        if (item != null) {
            respObj.put("Item", item);
        }
        return Response.status(Response.Status.BAD_REQUEST).entity(respObj).build();
    }

    private static Response getResponseForValidationException(ValidationException e) {
        Map<String, Object> respObj = new HashMap<>();
        respObj.put(ApiMetadata.EXCEPTION_TYPE, ApiMetadata.VALIDATION_EXCEPTION);
        respObj.put(ApiMetadata.EXCEPTION_MESSAGE, e.getMessage());
        return Response.status(Response.Status.BAD_REQUEST).entity(respObj).build();
    }

    private static Response getResponseForResourceInUseException(ResourceInUseException e) {
        Map<String, Object> respObj = new HashMap<>();
        respObj.put(ApiMetadata.EXCEPTION_TYPE, ApiMetadata.RESOURCE_IN_USE_EXCEPTION);
        respObj.put(ApiMetadata.EXCEPTION_MESSAGE, e.getMessage());
        return Response.status(Response.Status.BAD_REQUEST).entity(respObj).build();
    }

    private static String getOperationName(String api) {
        int lastDot = api.lastIndexOf('.');
        return api.substring(lastDot + 1);
    }

}

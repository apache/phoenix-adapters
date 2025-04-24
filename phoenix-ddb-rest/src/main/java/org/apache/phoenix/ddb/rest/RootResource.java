package org.apache.phoenix.ddb.rest;

import java.util.HashMap;
import java.util.Map;

import org.apache.hbase.thirdparty.javax.ws.rs.HeaderParam;
import org.apache.hbase.thirdparty.javax.ws.rs.POST;
import org.apache.hbase.thirdparty.javax.ws.rs.Path;
import org.apache.hbase.thirdparty.javax.ws.rs.Produces;
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
import org.apache.phoenix.ddb.service.PutItemService;
import org.apache.phoenix.ddb.service.QueryService;
import org.apache.phoenix.ddb.service.ScanService;
import org.apache.phoenix.ddb.service.utils.TableDescriptorUtils;
import org.apache.phoenix.ddb.service.utils.exceptions.ConditionCheckFailedException;
import org.apache.phoenix.ddb.utils.PhoenixUtils;

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
        PhoenixUtils.registerDriver();
    }

    @POST
    @Consumes({Constants.APPLICATION_AMZ_JSON, MediaType.APPLICATION_JSON})
    @Produces({Constants.APPLICATION_AMZ_JSON, MediaType.APPLICATION_JSON})
    public Response get(final @Context UriInfo uriInfo,
            final @HeaderParam("Content-Type") String contentType,
            final @HeaderParam("X-Amz-Target") String api,
            final Map<String, Object> request) {
        try {
            servlet.getMetrics().incrementRequests(1);
            final Map<String, Object> responseObject;

            switch (api) {
                case "DynamoDB_20120810.CreateTable": {
                    servlet.getMetrics().incrementCreateTableSuccessRequests(1);
                    responseObject = CreateTableService.createTable(request, jdbcConnectionUrl);
                    break;
                }
                case "DynamoDB_20120810.DescribeTable": {
                    responseObject = TableDescriptorUtils.getTableDescription(
                            (String) request.get("TableName"), jdbcConnectionUrl, "Table");
                    break;
                }
                case "DynamoDB_20120810.PutItem": {
                    try {
                        responseObject = PutItemService.putItem(request, jdbcConnectionUrl);
                    } catch (ConditionCheckFailedException e) {
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
                default: {
                    throw new IllegalArgumentException("Unknown API: " + api);
                }
            }

            ResponseBuilder response = Response.ok(responseObject);
            response.cacheControl(cacheControl);
            return response.build();
        } catch (Exception e) {
            LOG.error("Error ", e);
            switch (api) {
                case "DynamoDB_20120810.CreateTable": {
                    servlet.getMetrics().incrementCreateTableFailedRequests(1);
                    break;
                }
                case "DynamoDB_20120810.DescribeTable": {
                    break;
                }
                case "DynamoDB_20120810.PutItem": {
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
            throw e;
        }
    }

}

package org.apache.phoenix.ddb.rest.util;

public final class Constants {

    private Constants() {
    }

    public static final String APPLICATION_AMZ_JSON = "application/x-amz-json-1.0";

    public static final String PATH_SPEC_ANY = "/*";

    public static final int DEFAULT_LISTEN_PORT = 8080;
    public static final int DEFAULT_INFO_PORT = 8085;

    public static final String HTTP_HEADER_CACHE_SIZE = "phoenix.ddb.rest.http.header.cache.size";
    public static final int DEFAULT_HTTP_HEADER_CACHE_SIZE = Character.MAX_VALUE - 1;

    public static final String REST_HTTP_ALLOW_OPTIONS_METHOD =
            "phoenix.ddb.rest.http.allow.options.method";

    public static final String PHOENIX_DDB_REST_PORT = "phoenix.ddb.rest.port";
    public static final String PHOENIX_DDB_REST_INFO_PORT = "phoenix.ddb.rest.info.port";

    public static final String REST_DNS_INTERFACE = "phoenix.ddb.rest.dns.interface";
    public static final String REST_DNS_NAMESERVER = "phoenix.ddb.rest.dns.nameserver";

    public static final String PHOENIX_DDB_REST_INFO_BIND_ADDRESS =
            "phoenix.ddb.rest.info.bindAddress";
    public static final String DEFAULT_HOST = "0.0.0.0";

    public static final String REST_CONNECTOR_ACCEPT_QUEUE_SIZE =
            "phoenix.ddb.rest.connector.accept.queue.size";
    public static final String REST_THREAD_POOL_THREADS_MAX = "phoenix.ddb.rest.threads.max";
    public static final String REST_THREAD_POOL_THREADS_MIN = "phoenix.ddb.rest.threads.min";
    public static final String REST_THREAD_POOL_TASK_QUEUE_SIZE =
            "phoenix.ddb.rest.task.queue.size";
    public static final String REST_THREAD_POOL_THREAD_IDLE_TIMEOUT =
            "phoenix.ddb.rest.thread.idle.timeout";

    public static final String PHOENIX_DDB_ZK_QUORUM = "phoenix.ddb.zk.quorum";
    public static boolean REST_HTTP_ALLOW_OPTIONS_METHOD_DEFAULT = true;

    public static final int DEFAULT_HTTP_MAX_HEADER_SIZE = 64 * 2 * 1024;

    public static final String CLEANUP_INTERVAL = "phoenix.ddb.rest.connection.cleanup-interval";
    public static final String MAX_IDLETIME = "phoenix.ddb.rest.connection.max-idletime";
    public static final String SUPPORT_PROXY_USER = "phoenix.ddb.rest.support.proxyuser";

}

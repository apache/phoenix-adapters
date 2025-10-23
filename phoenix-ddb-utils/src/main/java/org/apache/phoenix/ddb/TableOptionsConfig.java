package org.apache.phoenix.ddb;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableOptionsConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableOptionsConfig.class);
    private static final String TABLE_CONFIG_FILE = "phoenix-table-options.properties";
    private static final String INDEX_CONFIG_FILE = "phoenix-index-options.properties";

    private static String tableOptionsString;
    private static String indexOptionsString;

    /**
     * Initialize both table and index configurations at startup.
     */
    public static void initialize() throws IOException {
        tableOptionsString = buildOptionsString(TABLE_CONFIG_FILE, "table");
        indexOptionsString = buildOptionsString(INDEX_CONFIG_FILE, "index");
        LOGGER.info("Initialized table and index configurations");
    }

    /**
     * Get table options as formatted string for CREATE statements.
     */
    public static String getTableOptions(boolean mergeEnabled) {
        if (tableOptionsString == null) {
            throw new IllegalStateException("Table Options Config not initialized.");
        }
        return tableOptionsString + ",MERGE_ENABLED=" + mergeEnabled;
    }

    /**
     * Get index options as formatted string for CREATE statements.
     */
    public static String getIndexOptions() {
        if (indexOptionsString == null) {
            throw new IllegalStateException("Index Options Config not initialized.");
        }
        return indexOptionsString;
    }

    /**
     * Load configuration from file and build formatted options string.
     */
    private static String buildOptionsString(String configFile, String configType)
            throws IOException {
        Properties props = loadConfiguration(configFile, configType);

        return String.join(",", props.stringPropertyNames().stream().map(key -> {
            // Handle quoted properties for HBase/Phoenix specific options
            if (key.contains(".")) {
                return "\"" + key + "\"=" + props.getProperty(key);
            } else {
                return key + "=" + props.getProperty(key);
            }
        }).toArray(String[]::new));
    }

    /**
     * Load configuration from the specified file.
     */
    private static Properties loadConfiguration(String configFile, String configType)
            throws IOException {
        Properties props = new Properties();
        try (InputStream is = TableOptionsConfig.class.getClassLoader()
                .getResourceAsStream(configFile)) {
            if (is != null) {
                props.load(is);
                LOGGER.info("Loaded {} options configuration from {}: {}", configType, configFile,
                        props);
            } else {
                throw new IOException("Configuration file not found: " + configFile);
            }
        }
        return props;
    }
}

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

package org.apache.phoenix.ddb.service.utils;

import java.util.List;

import org.apache.phoenix.schema.PColumn;

/**
 * Configuration container for scan operations
 */
public class ScanConfig {

    /**
     * Enumeration of different scan types
     */
    public enum ScanType {
        NO_EXCLUSIVE_START_KEY,           // Simple scan without pagination
        SINGLE_KEY_CONTINUATION,          // pk > value (single key table)
        TWO_KEY_FIRST_QUERY,             // pk1 = value AND pk2 > value (two key table, first query)
        TWO_KEY_SECOND_QUERY             // pk1 > value (two key table, second query)
    }

    private final ScanType type;
    private final boolean useIndex;
    private final List<PColumn> tablePKCols;
    private final List<PColumn> indexPKCols;
    private final PColumn partitionKeyCol;
    private final PColumn sortKeyCol;
    private final int limit;
    private final String tableName;
    private final String indexName;
    private final boolean countOnly;
    private boolean isSegmentScan = false;
    private ScanSegmentInfo scanSegmentInfo = null;

    public ScanConfig(ScanType type, boolean useIndex, List<PColumn> tablePKCols,
            List<PColumn> indexPKCols, int limit, String tableName, String indexName,
            boolean countOnly) {
        this.type = type;
        this.useIndex = useIndex;
        this.tablePKCols = tablePKCols;
        this.indexPKCols = indexPKCols;
        this.limit = limit;
        this.tableName = tableName;
        this.indexName = indexName;
        this.countOnly = countOnly;

        List<PColumn> relevantPKCols = useIndex ? indexPKCols : tablePKCols;
        this.partitionKeyCol = relevantPKCols.get(0);
        this.sortKeyCol = (relevantPKCols.size() > 1) ? relevantPKCols.get(1) : null;
    }

    /**
     * Creates a new ScanConfig with a different type and limit, keeping other properties
     */
    public ScanConfig cloneWithTypeAndLimit(ScanType newType, int newLimit) {
        ScanConfig newConfig = new ScanConfig(newType, this.useIndex, this.tablePKCols,
                this.indexPKCols, newLimit, this.tableName, this.indexName, this.countOnly);
        if (this.isSegmentScan) {
            newConfig.setScanSegmentInfo(this.scanSegmentInfo);
        }
        return newConfig;
    }

    public void setScanSegmentInfo(ScanSegmentInfo segmentInfo) {
        this.isSegmentScan = true;
        this.scanSegmentInfo = segmentInfo;
    }

    public boolean isSegmentScan() {
        return isSegmentScan && (scanSegmentInfo != null);
    }

    public ScanType getType() {
        return type;
    }

    public boolean useIndex() {
        return useIndex;
    }

    public List<PColumn> getTablePKCols() {
        return tablePKCols;
    }

    public List<PColumn> getIndexPKCols() {
        return indexPKCols;
    }

    public PColumn getPartitionKeyCol() {
        return partitionKeyCol;
    }

    public PColumn getSortKeyCol() {
        return sortKeyCol;
    }

    public int getLimit() {
        return limit;
    }

    public String getTableName() {
        return tableName;
    }

    public String getIndexName() {
        return indexName;
    }

    public boolean isCountOnly() {
        return countOnly;
    }

    public ScanSegmentInfo getScanSegmentInfo() {
        return scanSegmentInfo;
    }

    @Override
    public String toString() {
        if (isSegmentScan) {
            return this.type.toString() + "," + this.scanSegmentInfo.toShortString();
        } else {
            return this.type.toString();
        }
    }
}


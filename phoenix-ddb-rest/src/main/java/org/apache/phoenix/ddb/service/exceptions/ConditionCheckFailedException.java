package org.apache.phoenix.ddb.service.exceptions;

import java.util.Map;

public class ConditionCheckFailedException extends RuntimeException {

    private Map<String, Object> item = null;

    public ConditionCheckFailedException() {
        super();
    }

    public void setItem(Map<String, Object> item) {
        this.item = item;
    }

    public Map<String, Object> getItem() {
        return item;
    }
}

package org.apache.phoenix.ddb.bson;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;

public final class MapToBsonDocument {

    public static BsonDocument getBsonDocument(Map<String, Object> map) {
        BsonDocument document = new BsonDocument();
        updateDocWithMapAttributes(document, map);
        return document;
    }

    private static void updateDocWithMapAttributes(BsonDocument document, Map<String, Object> map) {
        map.forEach(
                (key, value) -> document.put(key, getValueFromMapVal((Map<String, Object>) value)));
    }

    private static BsonValue getValueFromMapVal(Map<String, Object> mapValue) {
        if (mapValue.size() != 1) {
            throw new IllegalArgumentException(
                    "Map size should be 1 for each key-value pair of " + "datatype");
        }
        if (mapValue.containsKey("S")) {
            return new BsonString((String) mapValue.get("S"));
        } else if (mapValue.containsKey("N")) {
            return BsonNumberConversionUtil.getBsonNumberFromNumber((String) mapValue.get("N"));
        } else if (mapValue.containsKey("B")) {
            return new BsonBinary(Base64.getDecoder().decode((String) mapValue.get("B")));
        } else if (mapValue.containsKey("BOOL")) {
            return new BsonBoolean((Boolean) mapValue.get("BOOL"));
        } else if (mapValue.containsKey("NULL")) {
            return new BsonNull();
        } else if (mapValue.containsKey("M")) {
            BsonDocument subDocument = new BsonDocument();
            updateDocWithMapAttributes(subDocument, (Map<String, Object>) mapValue.get("M"));
            return subDocument;
        } else if (mapValue.containsKey("L")) {
            BsonArray subArray = new BsonArray();
            updateListWithMapAttributes(subArray, (List<Object>) mapValue.get("L"));
            return subArray;
        } else if (mapValue.containsKey("SS")) {
            BsonDocument subDocument = new BsonDocument();
            BsonArray subArray = new BsonArray();
            for (String element : (List<String>) mapValue.get("SS")) {
                subArray.add(new BsonString(element));
            }
            subDocument.put("$set", subArray);
            return subDocument;
        } else if (mapValue.containsKey("NS")) {
            BsonDocument subDocument = new BsonDocument();
            BsonArray subArray = new BsonArray();
            for (String element : (List<String>) mapValue.get("NS")) {
                subArray.add(BsonNumberConversionUtil.getBsonNumberFromNumber(element));
            }
            subDocument.put("$set", subArray);
            return subDocument;
        } else if (mapValue.containsKey("BS")) {
            BsonDocument subDocument = new BsonDocument();
            BsonArray subArray = new BsonArray();
            for (String element : (List<String>) mapValue.get("BS")) {
                subArray.add(new BsonBinary(Base64.getDecoder().decode(element)));
            }
            subDocument.put("$set", subArray);
            return subDocument;
        } else {
            throw new IllegalStateException("Unexpected map data type: " + mapValue);
        }
    }

    private static void updateListWithMapAttributes(BsonArray subArray, List<Object> list) {
        for (Object obj : list) {
            subArray.add(getValueFromMapVal((Map<String, Object>) obj));
        }
    }

}

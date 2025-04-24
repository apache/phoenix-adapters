package org.apache.phoenix.ddb.bson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.phoenix.query.QueryConstants;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;

import java.util.Base64;
import java.util.Map;
import java.util.Optional;

public class CDCBsonUtil {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Get an array of RawBsonDocuments representing PRE and POST images of
     * a change in the given CDC JSON.
     * Return null element if a change does not have a pre/post image.
     */
    public static RawBsonDocument[] getBsonDocsForCDCImages(String cdcJson)
            throws JsonProcessingException {
        RawBsonDocument[] bsonDocs = new RawBsonDocument[2];
        Map<String, Object> map = OBJECT_MAPPER.readValue(cdcJson, Map.class);

        bsonDocs[0] = getBsonDocForKey(map, QueryConstants.CDC_PRE_IMAGE);
        bsonDocs[1] = getBsonDocForKey(map, QueryConstants.CDC_POST_IMAGE);

        // [pre, post]
        return bsonDocs;
    }

    private static RawBsonDocument getBsonDocForKey(Map<String, Object> map, String key) {
        return Optional.ofNullable(map.get(key))
                .map(m -> (Map<String, Object>) m)
                .map(m -> m.get("COL"))
                .map(String.class::cast)
                .map(Base64.getDecoder()::decode)
                .map(bytes -> new RawBsonDocument(bytes, 0, bytes.length))
                .orElse(null);
    }
}

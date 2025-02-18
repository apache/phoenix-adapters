import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.HashMap;
import java.util.Map;

public class GetRecordsBaseTest {

    public static Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", new AttributeValue().withS("ABC"));
        item.put("PK2", new AttributeValue().withN("3"));
        item.put("Id1", new AttributeValue().withN("-5"));
        item.put("Id2", new AttributeValue().withN("10.10"));
        item.put("title", new AttributeValue().withS("Title1"));
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", new AttributeValue().withS("Alice"));
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar", new AttributeValue().withL(new AttributeValue().withM(reviewMap1)));
        item.put("Reviews", new AttributeValue().withM(fiveStarMap));
        item.put("A.B", new AttributeValue().withS("not nested field 1"));
        return item;
    }

    public static Map<String, AttributeValue> getKey1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", new AttributeValue().withS("ABC"));
        item.put("PK2", new AttributeValue().withN("3"));
        return item;
    }

    public static Map<String, AttributeValue> getItem2() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", new AttributeValue().withS("XYZA"));
        item.put("PK2", new AttributeValue().withN("4"));
        item.put("Id1", new AttributeValue().withN("-5"));
        item.put("Id2", new AttributeValue().withN("10"));
        item.put("title", new AttributeValue().withS("Title1"));
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", new AttributeValue().withS("Bob"));
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar", new AttributeValue().withL(new AttributeValue().withM(reviewMap1)));
        item.put("Reviews", new AttributeValue().withM(fiveStarMap));
        item.put("A.B", new AttributeValue().withS("not nested field 1"));
        return item;
    }

    public static Map<String, AttributeValue> getKey2() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", new AttributeValue().withS("XYZA"));
        item.put("PK2", new AttributeValue().withN("4"));
        return item;
    }

    public static Map<String, AttributeValue> getItem3() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", new AttributeValue().withS("NEW"));
        item.put("PK2", new AttributeValue().withN("42"));
        item.put("Id1", new AttributeValue().withN("10"));
        return item;
    }

    public static Map<String, AttributeValue> getItem4() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", new AttributeValue().withS("OOO"));
        item.put("PK2", new AttributeValue().withN("4"));
        item.put("Id1", new AttributeValue().withN("0"));
        return item;
    }

    public static Map<String, AttributeValue> getItem5() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", new AttributeValue().withS("FOO"));
        item.put("PK2", new AttributeValue().withN("-22"));
        item.put("Id1", new AttributeValue().withN("100"));
        return item;
    }

    public static Map<String, AttributeValue> getItem6() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", new AttributeValue().withS("BAR"));
        item.put("PK2", new AttributeValue().withN("-22"));
        item.put("Id1", new AttributeValue().withN("100"));
        return item;
    }
}

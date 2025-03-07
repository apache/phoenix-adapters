import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.Map;

public class GetRecordsBaseTest {

    public static Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", AttributeValue.builder().s("ABC").build());
        item.put("PK2", AttributeValue.builder().n("3").build());
        item.put("Id1", AttributeValue.builder().n("-5").build());
        item.put("Id2", AttributeValue.builder().n("10.10").build());
        item.put("title", AttributeValue.builder().s("Title1").build());
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", AttributeValue.builder().s("Alice").build());
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar", AttributeValue.builder().l(AttributeValue.builder().m(reviewMap1).build()).build());
        item.put("Reviews", AttributeValue.builder().m(fiveStarMap).build());
        item.put("A.B", AttributeValue.builder().s("not nested field 1").build());
        return item;
    }

    public static Map<String, AttributeValue> getKey1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", AttributeValue.builder().s("ABC").build());
        item.put("PK2", AttributeValue.builder().n("3").build());
        return item;
    }

    public static Map<String, AttributeValue> getItem2() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", AttributeValue.builder().s("XYZA").build());
        item.put("PK2", AttributeValue.builder().n("4").build());
        item.put("Id1", AttributeValue.builder().n("-5").build());
        item.put("Id2", AttributeValue.builder().n("10").build());
        item.put("title", AttributeValue.builder().s("Title1").build());
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", AttributeValue.builder().s("Bob").build());
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar", AttributeValue.builder().l(AttributeValue.builder().m(reviewMap1).build()).build());
        item.put("Reviews", AttributeValue.builder().m(fiveStarMap).build());
        item.put("A.B", AttributeValue.builder().s("not nested field 1").build());
        return item;
    }

    public static Map<String, AttributeValue> getKey2() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", AttributeValue.builder().s("XYZA").build());
        item.put("PK2", AttributeValue.builder().n("4").build());
        return item;
    }

    public static Map<String, AttributeValue> getItem3() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", AttributeValue.builder().s("NEW").build());
        item.put("PK2", AttributeValue.builder().n("42").build());
        item.put("Id1", AttributeValue.builder().n("10").build());
        return item;
    }

    public static Map<String, AttributeValue> getItem4() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", AttributeValue.builder().s("OOO").build());
        item.put("PK2", AttributeValue.builder().n("4").build());
        item.put("Id1", AttributeValue.builder().n("0").build());
        return item;
    }

    public static Map<String, AttributeValue> getItem5() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", AttributeValue.builder().s("FOO").build());
        item.put("PK2", AttributeValue.builder().n("-22").build());
        item.put("Id1", AttributeValue.builder().n("100").build());
        return item;
    }

    public static Map<String, AttributeValue> getItem6() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", AttributeValue.builder().s("BAR").build());
        item.put("PK2", AttributeValue.builder().n("-22").build());
        item.put("Id1", AttributeValue.builder().n("100").build());
        return item;
    }
}

import org.apache.phoenix.ddb.utils.KeyConditionsHolder;
import org.junit.Test;
import org.junit.Assert;

import java.util.HashMap;
import java.util.Map;

public class KeyConditionsHolderTest {

    private Map<String, String> exprAttrNames = new HashMap<>();

    @Test
    public void test1() {
        String keyCondExpr = "#0 = :0";
        exprAttrNames.put("#0", "hashKey");
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr, exprAttrNames);
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertFalse(keyConditions.hasSortKey());
    }

    @Test
    public void test2() {
        String keyCondExpr = "#0 = :0 AND #1 = :1";
        exprAttrNames.put("#0", "hashKey");
        exprAttrNames.put("#1", "sortKey");
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr, exprAttrNames);
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("sortKey", keyConditions.getSortKeyName());
        Assert.assertEquals("=", keyConditions.getSortKeyOperator());
        Assert.assertEquals(":1", keyConditions.getSortKeyValue1());
    }

    @Test
    public void test3() {
        String keyCondExpr = "#0 = :0 AND #1 > :1";
        exprAttrNames.put("#0", "hashKey");
        exprAttrNames.put("#1", "sortKey");
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr, exprAttrNames);
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("sortKey", keyConditions.getSortKeyName());
        Assert.assertEquals(">", keyConditions.getSortKeyOperator());
        Assert.assertEquals(":1", keyConditions.getSortKeyValue1());
    }

    @Test
    public void test4() {
        String keyCondExpr = "#0 = :0 AND #1 < :1";
        exprAttrNames.put("#0", "hashKey");
        exprAttrNames.put("#1", "sortKey");
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr, exprAttrNames);
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("sortKey", keyConditions.getSortKeyName());
        Assert.assertEquals("<", keyConditions.getSortKeyOperator());
        Assert.assertEquals(":1", keyConditions.getSortKeyValue1());
    }

    @Test
    public void test5() {
        String keyCondExpr = "#0 = :0 AND #1 >= :1";
        exprAttrNames.put("#0", "hashKey");
        exprAttrNames.put("#1", "sortKey");
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr, exprAttrNames);
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("sortKey", keyConditions.getSortKeyName());
        Assert.assertEquals(">=", keyConditions.getSortKeyOperator());
        Assert.assertEquals(":1", keyConditions.getSortKeyValue1());
    }

    @Test
    public void test6() {
        String keyCondExpr = "#0 = :0 AND #1 <= :1";
        exprAttrNames.put("#0", "hashKey");
        exprAttrNames.put("#1", "sortKey");
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr, exprAttrNames);
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("sortKey", keyConditions.getSortKeyName());
        Assert.assertEquals("<=", keyConditions.getSortKeyOperator());
        Assert.assertEquals(":1", keyConditions.getSortKeyValue1());
    }

    @Test
    public void test7() {
        String keyCondExpr1 = "#0 = :0 AND #1 BETWEEN :1 AND :2";
        exprAttrNames.put("#0", "hashKey");
        exprAttrNames.put("#1", "sortKey");
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr1, exprAttrNames);
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("sortKey", keyConditions.getSortKeyName());
        Assert.assertTrue(keyConditions.hasBetween());
        Assert.assertEquals("BETWEEN", keyConditions.getSortKeyOperator());
        Assert.assertEquals(":1", keyConditions.getSortKeyValue1());
        Assert.assertEquals(":2", keyConditions.getSortKeyValue2());

        String keyCondExpr2 = "#hashKey = :hashKey AND #sortKey BETWEEN :sortKeyFrom AND :sortKeyTo";
        exprAttrNames.put("#hashKey", "Id");
        exprAttrNames.put("#sortKey", "Title");
        keyConditions = new KeyConditionsHolder(keyCondExpr2, exprAttrNames);
        Assert.assertEquals("Id", keyConditions.getPartitionKeyName());
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("Title", keyConditions.getSortKeyName());
        Assert.assertTrue(keyConditions.hasBetween());
        Assert.assertEquals("BETWEEN", keyConditions.getSortKeyOperator());
        Assert.assertEquals(":sortKeyFrom", keyConditions.getSortKeyValue1());
        Assert.assertEquals(":sortKeyTo", keyConditions.getSortKeyValue2());
    }

    @Test
    public void test8() {
        String keyCondExpr = "#0 = :0 AND begins_with(#1, :1)";
        exprAttrNames.put("#0", "hashKey");
        exprAttrNames.put("#1", "sortKey");
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr, exprAttrNames);
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("sortKey", keyConditions.getSortKeyName());
        Assert.assertTrue(keyConditions.hasBeginsWith());
        Assert.assertEquals(":1", keyConditions.getBeginsWithSortKeyVal());
    }

    @Test
    public void test9() {
        exprAttrNames = null;
        String keyCondExpr1 = "hashKey=:0 AND sortKey>:1";
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr1, exprAttrNames);
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("sortKey", keyConditions.getSortKeyName());
        Assert.assertEquals(">", keyConditions.getSortKeyOperator());
        Assert.assertEquals(":1", keyConditions.getSortKeyValue1());

        String keyCondExpr2 = "hashKey= :0 AND sortKey >:1";
        keyConditions = new KeyConditionsHolder(keyCondExpr2, exprAttrNames);
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("sortKey", keyConditions.getSortKeyName());
        Assert.assertEquals(">", keyConditions.getSortKeyOperator());
        Assert.assertEquals(":1", keyConditions.getSortKeyValue1());
        String keyCondExpr3 = "hashKey =:0 AND sortKey> :1";


        keyConditions = new KeyConditionsHolder(keyCondExpr3, exprAttrNames);
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("sortKey", keyConditions.getSortKeyName());
        Assert.assertEquals(">", keyConditions.getSortKeyOperator());
        Assert.assertEquals(":1", keyConditions.getSortKeyValue1());

        exprAttrNames = new HashMap<>();
    }
}

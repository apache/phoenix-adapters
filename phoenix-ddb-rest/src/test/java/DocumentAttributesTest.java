import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.BsonDocument;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.ddb.bson.BsonDocumentToMap;
import org.apache.phoenix.ddb.bson.DdbAttributesToBsonDocument;
import org.apache.phoenix.ddb.bson.MapToBsonDocument;

public class DocumentAttributesTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentAttributesTest.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test(timeout = 120000)
    public void test1() throws JsonProcessingException {
        verifyConversions(getItem1(), getItem1Json());
    }

    private static void verifyConversions(Map<String, AttributeValue> item, String expectedJson)
            throws JsonProcessingException {
        BsonDocument bsonDocument = DdbAttributesToBsonDocument.getBsonDocument(item);
        LOGGER.info("Item: {}", item);
        LOGGER.info("Converted document: {}", bsonDocument);
        Map<String, Object> map = BsonDocumentToMap.getFullItem(bsonDocument);
        LOGGER.info("Converted map: {}", map);
        String json = OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(map);
        LOGGER.info("Json: {}", json);
        Assert.assertEquals(expectedJson, json);
        BsonDocument convertedDoc = MapToBsonDocument.getBsonDocument(map);
        Assert.assertEquals(convertedDoc, bsonDocument);
    }

    @Test(timeout = 120000)
    public void test2() throws JsonProcessingException {
        verifyConversions(getItem2(), getItem2Json());
    }

    @Test(timeout = 120000)
    public void test3() throws JsonProcessingException {
        verifyConversions(getItem3(), getItem3Json());
    }

    @Test(timeout = 120000)
    public void test4() throws JsonProcessingException {
        verifyConversions(getItem4(), getItem4Json());
    }

    protected static Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", AttributeValue.builder().s("str_val_0").build());
        commonAttributeValues1(item);
        return item;
    }

    private static String getItem1Json() {
        return "{\n" + "  \"Pictures\" : {\n"
                + "    \"SS\" : [ \"http://example1.com/products/123_rear.jpg\", \"http://example2.com/products/xyz_rear.jpg\", \"http://example3.com/products/123_front.jpg\", \"http://example4.com/products/123_front.jpg\" ]\n"
                + "  },\n" + "  \"PictureBinarySet\" : {\n"
                + "    \"BS\" : [ \"aHR0cDovL2V4YW1wbGUxLmNvbS9wcm9kdWN0cy8xMjNfcmVhci5qcGc=\", \"aHR0cDovL2V4YW1wbGUyLmNvbS9wcm9kdWN0cy94eXpfcmVhci5qcGc=\", \"aHR0cDovL2V4YW1wbGUzLmNvbS9wcm9kdWN0cy8xMjNfZnJvbnQuanBn\", \"aHR0cDovL2V4YW1wbGU0LmNvbS9wcm9kdWN0cy8xMjNfZnJvbnQuanBn\" ]\n"
                + "  },\n" + "  \"Title\" : {\n" + "    \"S\" : \"Book 101 Title\"\n" + "  },\n"
                + "  \"InPublication\" : {\n" + "    \"BOOL\" : false\n" + "  },\n"
                + "  \"ColorBytes\" : {\n" + "    \"B\" : \"QmxhY2s=\"\n" + "  },\n"
                + "  \"ISBN\" : {\n" + "    \"S\" : \"111-1111111111\"\n" + "  },\n"
                + "  \"NestedList1\" : {\n" + "    \"L\" : [ {\n" + "      \"N\" : \"-485.34\"\n"
                + "    }, {\n" + "      \"S\" : \"1234abcd\"\n" + "    }, {\n"
                + "      \"L\" : [ {\n" + "        \"S\" : \"xyz0123\"\n" + "      }, {\n"
                + "        \"M\" : {\n" + "          \"InPublication\" : {\n"
                + "            \"BOOL\" : false\n" + "          },\n"
                + "          \"BinaryTitleSet\" : {\n"
                + "            \"BS\" : [ \"Qm9vayAxMDEwIFRpdGxlIEJpbmFyeQ==\", \"Qm9vayAxMDExIFRpdGxlIEJpbmFyeQ==\", \"Qm9vayAxMTExIFRpdGxlIEJpbmFyeQ==\" ]\n"
                + "          },\n" + "          \"ISBN\" : {\n"
                + "            \"S\" : \"111-1111111111\"\n" + "          },\n"
                + "          \"IdSet\" : {\n"
                + "            \"NS\" : [ \"204850.697038475\", \"-3947860.4863946\", \"20576024\", \"19306873\", \"4869067048362759068\" ]\n"
                + "          },\n" + "          \"Title\" : {\n"
                + "            \"S\" : \"Book 101 Title\"\n" + "          },\n"
                + "          \"Id\" : {\n" + "            \"N\" : \"101.01\"\n" + "          },\n"
                + "          \"TitleSet\" : {\n"
                + "            \"SS\" : [ \"Book 1010 Title\", \"Book 1011 Title\", \"Book 1111 Title\" ]\n"
                + "          }\n" + "        }\n" + "      } ]\n" + "    } ]\n" + "  },\n"
                + "  \"NestedMap1\" : {\n" + "    \"M\" : {\n" + "      \"InPublication\" : {\n"
                + "        \"BOOL\" : false\n" + "      },\n" + "      \"ISBN\" : {\n"
                + "        \"S\" : \"111-1111111111\"\n" + "      },\n" + "      \"Title\" : {\n"
                + "        \"S\" : \"Book 101 Title\"\n" + "      },\n" + "      \"Id\" : {\n"
                + "        \"N\" : \"101.01\"\n" + "      },\n" + "      \"NList1\" : {\n"
                + "        \"L\" : [ {\n" + "          \"S\" : \"NListVal01\"\n" + "        }, {\n"
                + "          \"N\" : \"-0.00234\"\n" + "        } ]\n" + "      }\n" + "    }\n"
                + "  },\n" + "  \"Id2\" : {\n" + "    \"N\" : \"101.01\"\n" + "  },\n"
                + "  \"attr_6\" : {\n" + "    \"M\" : {\n" + "      \"n_attr_0\" : {\n"
                + "        \"S\" : \"str_val_0\"\n" + "      },\n" + "      \"n_attr_1\" : {\n"
                + "        \"N\" : \"1295.03\"\n" + "      },\n" + "      \"n_attr_2\" : {\n"
                + "        \"B\" : \"MjA0OHU1bmJsd2plaVdGR1RIKDRiZjkzMA==\"\n" + "      },\n"
                + "      \"n_attr_3\" : {\n" + "        \"BOOL\" : true\n" + "      },\n"
                + "      \"n_attr_4\" : {\n" + "        \"NULL\" : true\n" + "      }\n" + "    }\n"
                + "  },\n" + "  \"attr_5\" : {\n" + "    \"L\" : [ {\n" + "      \"N\" : \"1234\"\n"
                + "    }, {\n" + "      \"S\" : \"str001\"\n" + "    }, {\n"
                + "      \"B\" : \"AAECAwQF\"\n" + "    } ]\n" + "  },\n" + "  \"IdS\" : {\n"
                + "    \"S\" : \"101.01\"\n" + "  },\n" + "  \"Id\" : {\n"
                + "    \"N\" : \"101.01\"\n" + "  },\n" + "  \"attr_1\" : {\n"
                + "    \"N\" : \"1295.03\"\n" + "  },\n" + "  \"attr_0\" : {\n"
                + "    \"S\" : \"str_val_0\"\n" + "  },\n" + "  \"RelatedItems\" : {\n"
                + "    \"NS\" : [ \"1234\", \"-485.45582904\", \"123.0948\", \"0.111\" ]\n"
                + "  }\n" + "}";
    }

    private static String getItem2Json() {
        return "{\n" + "  \"Pictures\" : {\n"
                + "    \"SS\" : [ \"1123_rear.jpg\", \"2xyz_rear.jpg\", \"3123_front.jpg\", \"4xyz_front.jpg\" ]\n"
                + "  },\n" + "  \"PictureBinarySet\" : {\n"
                + "    \"BS\" : [ \"MTEyM19yZWFyLmpwZw==\", \"Mnh5el9yZWFyLmpwZw==\", \"MzEyM19mcm9udC5qcGc=\", \"NHh5el9mcm9udC5qcGc=\", \"NTEyM2FiY19yZWFyLmpwZw==\", \"Nnh5emFiY19yZWFyLmpwZw==\" ]\n"
                + "  },\n" + "  \"Title\" : {\n" + "    \"S\" : \"Book 101 Title\"\n" + "  },\n"
                + "  \"InPublication\" : {\n" + "    \"BOOL\" : false\n" + "  },\n"
                + "  \"ColorBytes\" : {\n" + "    \"B\" : \"QmxhY2s=\"\n" + "  },\n"
                + "  \"ISBN\" : {\n" + "    \"S\" : \"111-1111111111\"\n" + "  },\n"
                + "  \"NestedList1\" : {\n" + "    \"L\" : [ {\n" + "      \"N\" : \"-485.34\"\n"
                + "    }, {\n" + "      \"S\" : \"1234abcd\"\n" + "    }, {\n"
                + "      \"L\" : [ {\n" + "        \"S\" : \"xyz0123\"\n" + "      }, {\n"
                + "        \"M\" : {\n" + "          \"InPublication\" : {\n"
                + "            \"BOOL\" : false\n" + "          },\n"
                + "          \"BinaryTitleSet\" : {\n"
                + "            \"BS\" : [ \"Qm9vayAxMDEwIFRpdGxlIEJpbmFyeQ==\", \"Qm9vayAxMDExIFRpdGxlIEJpbmFyeQ==\", \"Qm9vayAxMTExIFRpdGxlIEJpbmFyeQ==\" ]\n"
                + "          },\n" + "          \"TitleSet1\" : {\n"
                + "            \"SS\" : [ \"Book 1010 Title\", \"Book 1011 Title\", \"Book 1111 Title\", \"Book 1200 Title\", \"Book 1201 Title\" ]\n"
                + "          },\n" + "          \"ISBN\" : {\n"
                + "            \"S\" : \"111-1111111111\"\n" + "          },\n"
                + "          \"IdSet\" : {\n"
                + "            \"NS\" : [ \"204850.69703847\", \"-3945786.4869476\", \"20576024\", \"19306873\", \"486906704836274959\" ]\n"
                + "          },\n" + "          \"Title\" : {\n"
                + "            \"S\" : \"Book 101 Title\"\n" + "          },\n"
                + "          \"Id\" : {\n" + "            \"N\" : \"9991234.0231\"\n"
                + "          },\n" + "          \"TitleSet2\" : {\n"
                + "            \"SS\" : [ \"Book 1010 Title\", \"Book 1011 Title\", \"Book 1111 Title\", \"Book 1200 Title\", \"Book 1201 Title\" ]\n"
                + "          }\n" + "        }\n" + "      } ]\n" + "    } ]\n" + "  },\n"
                + "  \"NestedMap1\" : {\n" + "    \"M\" : {\n" + "      \"InPublication\" : {\n"
                + "        \"BOOL\" : false\n" + "      },\n" + "      \"ISBN\" : {\n"
                + "        \"S\" : \"111-1111111111\"\n" + "      },\n"
                + "      \"NestedMap2\" : {\n" + "        \"M\" : {\n"
                + "          \"InPublication\" : {\n" + "            \"BOOL\" : true\n"
                + "          },\n" + "          \"NList\" : {\n" + "            \"L\" : [ {\n"
                + "              \"S\" : \"NListVal01\"\n" + "            }, {\n"
                + "              \"N\" : \"-0.00234\"\n" + "            } ]\n" + "          },\n"
                + "          \"ISBN\" : {\n" + "            \"S\" : \"111-1111111111999\"\n"
                + "          },\n" + "          \"Title\" : {\n"
                + "            \"S\" : \"Book 10122 Title\"\n" + "          },\n"
                + "          \"Id\" : {\n" + "            \"N\" : \"101.22\"\n" + "          }\n"
                + "        }\n" + "      },\n" + "      \"Title\" : {\n"
                + "        \"S\" : \"Book 101 Title\"\n" + "      },\n" + "      \"Id\" : {\n"
                + "        \"N\" : \"101.01\"\n" + "      },\n" + "      \"NList1\" : {\n"
                + "        \"L\" : [ {\n" + "          \"S\" : \"NListVal01\"\n" + "        }, {\n"
                + "          \"N\" : \"-0.00234\"\n" + "        }, {\n"
                + "          \"B\" : \"dG9fYmVfcmVtb3ZlZA==\"\n" + "        } ]\n" + "      },\n"
                + "      \"NSet1\" : {\n"
                + "        \"NS\" : [ \"123.45\", \"9586.7778\", \"-124\", \"-6830.5555\", \"10238\", \"-48695\" ]\n"
                + "      }\n" + "    }\n" + "  },\n" + "  \"Id2\" : {\n"
                + "    \"N\" : \"101234.9921\"\n" + "  },\n" + "  \"attr_6\" : {\n"
                + "    \"M\" : {\n" + "      \"n_attr_0\" : {\n" + "        \"S\" : \"str_val_0\"\n"
                + "      },\n" + "      \"n_attr_1\" : {\n" + "        \"N\" : \"1295.03\"\n"
                + "      },\n" + "      \"n_attr_2\" : {\n"
                + "        \"B\" : \"MjA0OHU1bmJsd2plaVdGR1RIKDRiZjkzMA==\"\n" + "      },\n"
                + "      \"n_attr_3\" : {\n" + "        \"BOOL\" : true\n" + "      },\n"
                + "      \"n_attr_4\" : {\n" + "        \"NULL\" : true\n" + "      }\n" + "    }\n"
                + "  },\n" + "  \"attr_5\" : {\n" + "    \"L\" : [ {\n" + "      \"N\" : \"1234\"\n"
                + "    }, {\n" + "      \"S\" : \"str001\"\n" + "    }, {\n"
                + "      \"B\" : \"AAECAwQF\"\n" + "    } ]\n" + "  },\n"
                + "  \"NestedList12\" : {\n" + "    \"L\" : [ {\n" + "      \"N\" : \"-485.34\"\n"
                + "    }, {\n" + "      \"S\" : \"1234abcd\"\n" + "    }, {\n"
                + "      \"L\" : [ {\n" + "        \"SS\" : [ \"xyz0123\" ]\n" + "      }, {\n"
                + "        \"BS\" : [ \"dmFsMDE=\", \"dmFsMDI=\", \"dmFsMDM=\" ]\n" + "      } ]\n"
                + "    } ]\n" + "  },\n" + "  \"IdS\" : {\n" + "    \"S\" : \"101.01\"\n" + "  },\n"
                + "  \"Id\" : {\n" + "    \"N\" : \"-102.01\"\n" + "  },\n" + "  \"attr_1\" : {\n"
                + "    \"N\" : \"1295.03\"\n" + "  },\n" + "  \"attr_0\" : {\n"
                + "    \"S\" : \"str_val_0\"\n" + "  },\n" + "  \"RelatedItems\" : {\n"
                + "    \"NS\" : [ \"1234\", \"-485.45582904\", \"123.0948\", \"0.111\" ]\n"
                + "  }\n" + "}";
    }

    private static String getItem3Json() {
        return "{\n" + "  \"Pictures\" : {\n"
                + "    \"SS\" : [ \"123_rear.jpg\", \"1235@_rear.jpg\", \"xyz5@_rear.jpg\", \"xyz_rear.jpg\", \"123_front.jpg\", \"xyz_front.jpg\" ]\n"
                + "  },\n" + "  \"PictureBinarySet\" : {\n"
                + "    \"BS\" : [ \"eHl6X3JlYXIuanBn\", \"MTIzX2Zyb250LmpwZw==\", \"MTIzYWJjX3JlYXIuanBn\", \"eHl6YWJjX3JlYXIuanBn\" ]\n"
                + "  },\n" + "  \"Title\" : {\n" + "    \"S\" : \"Cycle_1234_new\"\n" + "  },\n"
                + "  \"InPublication\" : {\n" + "    \"BOOL\" : false\n" + "  },\n"
                + "  \"AddedId\" : {\n" + "    \"N\" : \"10\"\n" + "  },\n"
                + "  \"ColorBytes\" : {\n" + "    \"B\" : \"QmxhY2s=\"\n" + "  },\n"
                + "  \"ISBN\" : {\n" + "    \"S\" : \"111-1111111111\"\n" + "  },\n"
                + "  \"NestedList1\" : {\n" + "    \"L\" : [ {\n" + "      \"N\" : \"-485.34\"\n"
                + "    }, {\n" + "      \"S\" : \"1234abcd\"\n" + "    }, {\n"
                + "      \"L\" : [ {\n" + "        \"S\" : \"xyz0123\"\n" + "      }, {\n"
                + "        \"M\" : {\n" + "          \"InPublication\" : {\n"
                + "            \"BOOL\" : false\n" + "          },\n"
                + "          \"BinaryTitleSet\" : {\n"
                + "            \"BS\" : [ \"Qm9vayAxMDEwIFRpdGxlIEJpbmFyeQ==\", \"Qm9vayAxMDExIFRpdGxlIEJpbmFyeQ==\", \"Qm9vayAxMTExIFRpdGxlIEJpbmFyeQ==\" ]\n"
                + "          },\n" + "          \"ISBN\" : {\n"
                + "            \"S\" : \"111-1111111122\"\n" + "          },\n"
                + "          \"IdSet\" : {\n"
                + "            \"NS\" : [ \"204850.697038423\", \"-9457860.98693947\", \"20576024\", \"19306873\", \"9067048362749590684\" ]\n"
                + "          },\n" + "          \"Title\" : {\n"
                + "            \"S\" : \"Book 101 Title\"\n" + "          },\n"
                + "          \"Id\" : {\n" + "            \"N\" : \"101.01\"\n" + "          },\n"
                + "          \"TitleSet2\" : {\n"
                + "            \"SS\" : [ \"Book 1111 Title\", \"Book 1200 Title\", \"Book 1201 Title\" ]\n"
                + "          }\n" + "        }\n" + "      } ]\n" + "    } ]\n" + "  },\n"
                + "  \"NestedMap1\" : {\n" + "    \"M\" : {\n" + "      \"InPublication\" : {\n"
                + "        \"BOOL\" : false\n" + "      },\n" + "      \"ColorList\" : {\n"
                + "        \"L\" : [ {\n" + "          \"S\" : \"Black\"\n" + "        }, {\n"
                + "          \"B\" : \"V2hpdGU=\"\n" + "        }, {\n"
                + "          \"S\" : \"Silver\"\n" + "        } ]\n" + "      },\n"
                + "      \"AddedId\" : {\n" + "        \"N\" : \"10\"\n" + "      },\n"
                + "      \"ISBN\" : {\n" + "        \"S\" : \"111-1111111111\"\n" + "      },\n"
                + "      \"NestedMap2\" : {\n" + "        \"M\" : {\n" + "          \"NewID\" : {\n"
                + "            \"S\" : \"12345\"\n" + "          },\n" + "          \"NList\" : {\n"
                + "            \"L\" : [ {\n" + "              \"N\" : \"12.22\"\n"
                + "            }, {\n" + "              \"N\" : \"-0.00234\"\n"
                + "            }, {\n" + "              \"NULL\" : true\n" + "            } ]\n"
                + "          },\n" + "          \"ISBN\" : {\n"
                + "            \"S\" : \"111-1111111111999\"\n" + "          },\n"
                + "          \"Title\" : {\n" + "            \"S\" : \"Book 10122 Title\"\n"
                + "          },\n" + "          \"Id\" : {\n"
                + "            \"N\" : \"-12243.78\"\n" + "          }\n" + "        }\n"
                + "      },\n" + "      \"Id\" : {\n" + "        \"N\" : \"101.01\"\n"
                + "      },\n" + "      \"NList1\" : {\n" + "        \"L\" : [ {\n"
                + "          \"SS\" : [ \"Updated_set_01\", \"Updated_set_02\" ]\n"
                + "        }, {\n" + "          \"N\" : \"-0.00234\"\n" + "        } ]\n"
                + "      },\n" + "      \"NSet1\" : {\n"
                + "        \"NS\" : [ \"123.45\", \"9586.7778\", \"-124\", \"10238\" ]\n"
                + "      }\n" + "    }\n" + "  },\n" + "  \"Id1\" : {\n"
                + "    \"B\" : \"SURfMTAx\"\n" + "  },\n" + "  \"attr_6\" : {\n" + "    \"M\" : {\n"
                + "      \"n_attr_0\" : {\n" + "        \"S\" : \"str_val_0\"\n" + "      },\n"
                + "      \"n_attr_1\" : {\n" + "        \"N\" : \"1295.03123\"\n" + "      },\n"
                + "      \"n_attr_2\" : {\n"
                + "        \"B\" : \"MjA0OHU1bmJsd2plaVdGR1RIKDRiZjkzMA==\"\n" + "      },\n"
                + "      \"n_attr_3\" : {\n" + "        \"BOOL\" : true\n" + "      },\n"
                + "      \"n_attr_4\" : {\n" + "        \"NULL\" : true\n" + "      }\n" + "    }\n"
                + "  },\n" + "  \"attr_5\" : {\n" + "    \"L\" : [ {\n" + "      \"N\" : \"1234\"\n"
                + "    }, {\n" + "      \"S\" : \"str001\"\n" + "    }, {\n"
                + "      \"B\" : \"AAECAwQF\"\n" + "    } ]\n" + "  },\n"
                + "  \"NestedList12\" : {\n" + "    \"L\" : [ {\n" + "      \"N\" : \"-485.34\"\n"
                + "    }, {\n" + "      \"S\" : \"1234abcd\"\n" + "    }, {\n"
                + "      \"L\" : [ {\n"
                + "        \"SS\" : [ \"xyz0123\", \"xyz01234\", \"abc01234\" ]\n" + "      }, {\n"
                + "        \"BS\" : [ \"dmFsMDE=\", \"dmFsMDI=\", \"dmFsMDM=\", \"dmFsMDQ=\" ]\n"
                + "      }, {\n"
                + "        \"NS\" : [ \"-234.56\", \"123\", \"93756.93475960549\", \"9375572.6870899\" ]\n"
                + "      } ]\n" + "    } ]\n" + "  },\n" + "  \"Id\" : {\n"
                + "    \"S\" : \"12345\"\n" + "  },\n" + "  \"attr_1\" : {\n"
                + "    \"N\" : \"1295.03\"\n" + "  },\n" + "  \"attr_0\" : {\n"
                + "    \"S\" : \"str_val_0\"\n" + "  },\n" + "  \"RelatedItems\" : {\n"
                + "    \"NS\" : [ \"1234\", \"-485.45582904\", \"123.0948\", \"0.111\" ]\n"
                + "  }\n" + "}";
    }

    private static String getItem4Json() {
        return "{\n" + "  \"Pictures\" : {\n"
                + "    \"SS\" : [ \"123_rear.jpg\", \"1235@_rear.jpg\", \"xyz5@_rear.jpg\", \"xyz_rear.jpg\", \"123_front.jpg\", \"xyz_front.jpg\" ]\n"
                + "  },\n" + "  \"PictureBinarySet\" : {\n"
                + "    \"BS\" : [ \"eHl6X3JlYXIuanBn\", \"MTIzX2Zyb250LmpwZw==\", \"MTIzYWJjX3JlYXIuanBn\", \"eHl6YWJjX3JlYXIuanBn\" ]\n"
                + "  },\n" + "  \"Title\" : {\n" + "    \"S\" : \"Cycle_1234_new\"\n" + "  },\n"
                + "  \"InPublication\" : {\n" + "    \"BOOL\" : false\n" + "  },\n"
                + "  \"AddedId\" : {\n" + "    \"N\" : \"10\"\n" + "  },\n"
                + "  \"ColorBytes\" : {\n" + "    \"B\" : \"QmxhY2s=\"\n" + "  },\n"
                + "  \"ISBN\" : {\n" + "    \"S\" : \"111-1111111111\"\n" + "  },\n"
                + "  \"NestedList1\" : {\n" + "    \"L\" : [ {\n"
                + "      \"N\" : \"-473.11999999999995\"\n" + "    }, {\n"
                + "      \"S\" : \"1234abcd\"\n" + "    }, {\n" + "      \"L\" : [ {\n"
                + "        \"S\" : \"xyz0123\"\n" + "      }, {\n" + "        \"M\" : {\n"
                + "          \"InPublication\" : {\n" + "            \"BOOL\" : false\n"
                + "          },\n" + "          \"BinaryTitleSet\" : {\n"
                + "            \"BS\" : [ \"Qm9vayAxMDEwIFRpdGxlIEJpbmFyeQ==\", \"Qm9vayAxMDExIFRpdGxlIEJpbmFyeQ==\", \"Qm9vayAxMTExIFRpdGxlIEJpbmFyeQ==\" ]\n"
                + "          },\n" + "          \"ISBN\" : {\n"
                + "            \"S\" : \"111-1111111122\"\n" + "          },\n"
                + "          \"IdSet\" : {\n"
                + "            \"NS\" : [ \"2048502.69703847\", \"-45786.4869476\", \"20576024\", \"19306873\", \"83627495940380684\" ]\n"
                + "          },\n" + "          \"Title\" : {\n"
                + "            \"S\" : \"Book 101 Title\"\n" + "          },\n"
                + "          \"Id\" : {\n" + "            \"N\" : \"101.01\"\n" + "          },\n"
                + "          \"TitleSet2\" : {\n"
                + "            \"SS\" : [ \"Book 1111 Title\", \"Book 1200 Title\", \"Book 1201 Title\" ]\n"
                + "          }\n" + "        }\n" + "      } ]\n" + "    }, {\n"
                + "      \"NULL\" : true\n" + "    }, {\n" + "      \"BOOL\" : true\n" + "    } ]\n"
                + "  },\n" + "  \"NestedMap1\" : {\n" + "    \"M\" : {\n"
                + "      \"InPublication\" : {\n" + "        \"BOOL\" : false\n" + "      },\n"
                + "      \"ColorList\" : {\n" + "        \"L\" : [ {\n"
                + "          \"S\" : \"Black\"\n" + "        }, {\n"
                + "          \"B\" : \"V2hpdGU=\"\n" + "        }, {\n"
                + "          \"S\" : \"Silver\"\n" + "        } ]\n" + "      },\n"
                + "      \"AddedId\" : {\n" + "        \"N\" : \"10\"\n" + "      },\n"
                + "      \"ISBN\" : {\n" + "        \"S\" : \"111-1111111111\"\n" + "      },\n"
                + "      \"NestedMap2\" : {\n" + "        \"M\" : {\n" + "          \"NewID\" : {\n"
                + "            \"S\" : \"12345\"\n" + "          },\n" + "          \"NList\" : {\n"
                + "            \"L\" : [ {\n" + "              \"N\" : \"12.22\"\n"
                + "            }, {\n" + "              \"N\" : \"-0.00234\"\n"
                + "            }, {\n" + "              \"NULL\" : true\n" + "            } ]\n"
                + "          },\n" + "          \"ISBN\" : {\n"
                + "            \"S\" : \"111-1111111111999\"\n" + "          },\n"
                + "          \"Title\" : {\n" + "            \"S\" : \"Book 10122 Title\"\n"
                + "          },\n" + "          \"Id\" : {\n"
                + "            \"N\" : \"-12243.78\"\n" + "          }\n" + "        }\n"
                + "      },\n" + "      \"Id\" : {\n" + "        \"N\" : \"101.01\"\n"
                + "      },\n" + "      \"NList1\" : {\n" + "        \"L\" : [ {\n"
                + "          \"SS\" : [ \"Updated_set_01\", \"Updated_set_02\" ]\n"
                + "        }, {\n" + "          \"N\" : \"-0.00234\"\n" + "        } ]\n"
                + "      },\n" + "      \"NSet1\" : {\n"
                + "        \"NS\" : [ \"123.45\", \"9586.7778\", \"-124\", \"10238\", \"-0.29475029304\" ]\n"
                + "      }\n" + "    }\n" + "  },\n" + "  \"Id1\" : {\n" + "    \"S\" : \"12345\"\n"
                + "  },\n" + "  \"attr_6\" : {\n" + "    \"M\" : {\n" + "      \"n_attr_0\" : {\n"
                + "        \"S\" : \"str_val_0\"\n" + "      },\n" + "      \"n_attr_1\" : {\n"
                + "        \"N\" : \"1223456.03\"\n" + "      },\n" + "      \"n_attr_2\" : {\n"
                + "        \"B\" : \"MjA0OHU1bmJsd2plaVdGR1RIKDRiZjkzMA==\"\n" + "      },\n"
                + "      \"n_attr_3\" : {\n" + "        \"BOOL\" : true\n" + "      },\n"
                + "      \"n_attr_4\" : {\n" + "        \"NULL\" : true\n" + "      }\n" + "    }\n"
                + "  },\n" + "  \"attr_5\" : {\n" + "    \"L\" : [ {\n" + "      \"N\" : \"1224\"\n"
                + "    }, {\n" + "      \"S\" : \"str001\"\n" + "    }, {\n"
                + "      \"B\" : \"AAECAwQF\"\n" + "    } ]\n" + "  },\n"
                + "  \"NestedList12\" : {\n" + "    \"L\" : [ {\n" + "      \"N\" : \"-485.34\"\n"
                + "    }, {\n" + "      \"S\" : \"1234abcd\"\n" + "    }, {\n"
                + "      \"L\" : [ {\n"
                + "        \"SS\" : [ \"xyz0123\", \"xyz01234\", \"abc01234\" ]\n" + "      }, {\n"
                + "        \"BS\" : [ \"dmFsMDE=\", \"dmFsMDI=\", \"dmFsMDM=\", \"dmFsMDQ=\" ]\n"
                + "      } ]\n" + "    } ]\n" + "  },\n" + "  \"Id\" : {\n"
                + "    \"S\" : \"12345\"\n" + "  },\n" + "  \"attr_1\" : {\n"
                + "    \"N\" : \"1295.03\"\n" + "  },\n" + "  \"attr_0\" : {\n"
                + "    \"S\" : \"str_val_0\"\n" + "  },\n" + "  \"RelatedItems\" : {\n"
                + "    \"NS\" : [ \"1234\", \"-485.45582904\", \"123.0948\", \"0.111\" ]\n"
                + "  }\n" + "}";
    }

    protected static Map<String, AttributeValue> getItemWithBinary1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0",
                AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("str_val_0")))
                        .build());
        item.put("pk2",
                AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("str_val_2")))
                        .build());
        commonAttributeValues1(item);
        return item;
    }

    private static void commonAttributeValues1(Map<String, AttributeValue> item) {
        item.put("attr_1", AttributeValue.builder().n("1295.03").build());
        item.put("attr_5", AttributeValue.builder().l(AttributeValue.builder().n("1234").build(),
                AttributeValue.builder().s("str001").build(),
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {0, 1, 2, 3, 4, 5}))
                        .build()).build());
        Map<String, AttributeValue> nMap1 = new HashMap<>();
        nMap1.put("n_attr_0", AttributeValue.builder().s("str_val_0").build());
        nMap1.put("n_attr_1", AttributeValue.builder().n("1295.03").build());
        String bytesFieldVal1 = "2048u5nblwjeiWFGTH(4bf930";
        byte[] bytesAttrVal1 = bytesFieldVal1.getBytes();
        nMap1.put("n_attr_2",
                AttributeValue.builder().b(SdkBytes.fromByteArray(bytesAttrVal1)).build());
        nMap1.put("n_attr_3", AttributeValue.builder().bool(true).build());
        nMap1.put("n_attr_4", AttributeValue.builder().nul(true).build());
        item.put("attr_6", AttributeValue.builder().m(nMap1).build());
        item.put("Id", AttributeValue.builder().n("101.01").build());
        item.put("IdS", AttributeValue.builder().s("101.01").build());
        item.put("Id2", AttributeValue.builder().n("101.01").build());
        item.put("ColorBytes",
                AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("Black"))).build());
        item.put("RelatedItems",
                AttributeValue.builder().ns("1234", "-485.45582904", "123.0948", "0.111").build());
        item.put("Pictures", AttributeValue.builder()
                .ss("http://example1.com/products/123_rear.jpg",
                        "http://example2.com/products/xyz_rear.jpg",
                        "http://example3.com/products/123_front.jpg",
                        "http://example4.com/products/123_front.jpg").build());
        item.put("PictureBinarySet", AttributeValue.builder().bs(SdkBytes.fromByteArray(
                                Bytes.toBytes("http://example1.com/products/123_rear.jpg")),
                        SdkBytes.fromByteArray(Bytes.toBytes("http://example2.com/products/xyz_rear.jpg")),
                        SdkBytes.fromByteArray(Bytes.toBytes("http://example3.com/products/123_front.jpg")),
                        SdkBytes.fromByteArray(Bytes.toBytes("http://example4.com/products/123_front.jpg")))
                .build());
        item.put("Title", AttributeValue.builder().s("Book 101 Title").build());
        item.put("ISBN", AttributeValue.builder().s("111-1111111111").build());
        item.put("InPublication", AttributeValue.builder().bool(false).build());
        Map<String, AttributeValue> nestedMap1 = new HashMap<>();
        nestedMap1.put("Id", AttributeValue.builder().n("101.01").build());
        nestedMap1.put("Title", AttributeValue.builder().s("Book 101 Title").build());
        nestedMap1.put("ISBN", AttributeValue.builder().s("111-1111111111").build());
        nestedMap1.put("InPublication", AttributeValue.builder().bool(false).build());
        nestedMap1.put("NList1", AttributeValue.builder()
                .l(AttributeValue.builder().s("NListVal01").build(),
                        AttributeValue.builder().n("-0.00234").build()).build());
        item.put("NestedMap1", AttributeValue.builder().m(nestedMap1).build());
        Map<String, AttributeValue> nestedList1Map1 = new HashMap<>();
        nestedList1Map1.put("Id", AttributeValue.builder().n("101.01").build());
        nestedList1Map1.put("Title", AttributeValue.builder().s("Book 101 Title").build());
        nestedList1Map1.put("ISBN", AttributeValue.builder().s("111-1111111111").build());
        nestedList1Map1.put("InPublication", AttributeValue.builder().bool(false).build());
        nestedList1Map1.put("IdSet", AttributeValue.builder()
                .ns("204850.697038475", "-3947860.4863946", "20576024", "19306873",
                        "4869067048362759068").build());
        nestedList1Map1.put("TitleSet",
                AttributeValue.builder().ss("Book 1010 Title", "Book 1011 Title", "Book 1111 Title")
                        .build());
        nestedList1Map1.put("BinaryTitleSet", AttributeValue.builder()
                .bs(SdkBytes.fromByteArray(Bytes.toBytes("Book 1010 Title Binary")),
                        SdkBytes.fromByteArray(Bytes.toBytes("Book 1011 Title Binary")),
                        SdkBytes.fromByteArray(Bytes.toBytes("Book 1111 Title Binary"))).build());
        item.put("NestedList1", AttributeValue.builder()
                .l(AttributeValue.builder().n("-485.34").build(),
                        AttributeValue.builder().s("1234abcd").build(), AttributeValue.builder()
                                .l(AttributeValue.builder().s("xyz0123").build(),
                                        AttributeValue.builder().m(nestedList1Map1).build())
                                .build()).build());
    }

    protected static Map<String, AttributeValue> getItem2() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", AttributeValue.builder().s("str_val_0").build());
        commonAttributeValues2(item);
        return item;
    }

    protected static Map<String, AttributeValue> getItemWithBinary2() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0",
                AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("str_val_0")))
                        .build());
        item.put("pk2",
                AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("str_val_2")))
                        .build());
        commonAttributeValues2(item);
        return item;
    }

    private static void commonAttributeValues2(Map<String, AttributeValue> item) {
        item.put("attr_1", AttributeValue.builder().n("1295.03").build());
        item.put("attr_5", AttributeValue.builder().l(AttributeValue.builder().n("1234").build(),
                AttributeValue.builder().s("str001").build(),
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {0, 1, 2, 3, 4, 5}))
                        .build()).build());
        Map<String, AttributeValue> nMap1 = new HashMap<>();
        nMap1.put("n_attr_0", AttributeValue.builder().s("str_val_0").build());
        nMap1.put("n_attr_1", AttributeValue.builder().n("1295.03").build());
        String bytesAttributeVal1 = "2048u5nblwjeiWFGTH(4bf930";
        byte[] bytesAttrVal1 = bytesAttributeVal1.getBytes();
        nMap1.put("n_attr_2",
                AttributeValue.builder().b(SdkBytes.fromByteArray(bytesAttrVal1)).build());
        nMap1.put("n_attr_3", AttributeValue.builder().bool(true).build());
        nMap1.put("n_attr_4", AttributeValue.builder().nul(true).build());
        item.put("attr_6", AttributeValue.builder().m(nMap1).build());
        item.put("Id", AttributeValue.builder().n("-102.01").build());
        item.put("IdS", AttributeValue.builder().s("101.01").build());
        item.put("Id2", AttributeValue.builder().n("101234.9921").build());
        item.put("ColorBytes",
                AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("Black"))).build());
        item.put("RelatedItems",
                AttributeValue.builder().ns("1234", "-485.45582904", "123.0948", "0.111").build());
        item.put("Pictures", AttributeValue.builder()
                .ss("1123_rear.jpg", "2xyz_rear.jpg", "3123_front.jpg", "4xyz_front.jpg").build());
        item.put("PictureBinarySet", AttributeValue.builder()
                .bs(SdkBytes.fromByteArray(Bytes.toBytes("1123_rear.jpg")),
                        SdkBytes.fromByteArray(Bytes.toBytes("2xyz_rear.jpg")),
                        SdkBytes.fromByteArray(Bytes.toBytes("3123_front.jpg")),
                        SdkBytes.fromByteArray(Bytes.toBytes("4xyz_front.jpg")),
                        SdkBytes.fromByteArray(Bytes.toBytes("5123abc_rear.jpg")),
                        SdkBytes.fromByteArray(Bytes.toBytes("6xyzabc_rear.jpg"))).build());
        item.put("Title", AttributeValue.builder().s("Book 101 Title").build());
        item.put("ISBN", AttributeValue.builder().s("111-1111111111").build());
        item.put("InPublication", AttributeValue.builder().bool(false).build());
        Map<String, AttributeValue> nestedMap1 = new HashMap<>();
        nestedMap1.put("Id", AttributeValue.builder().n("101.01").build());
        nestedMap1.put("Title", AttributeValue.builder().s("Book 101 Title").build());
        nestedMap1.put("ISBN", AttributeValue.builder().s("111-1111111111").build());
        nestedMap1.put("InPublication", AttributeValue.builder().bool(false).build());
        nestedMap1.put("NList1", AttributeValue.builder()
                .l(AttributeValue.builder().s("NListVal01").build(),
                        AttributeValue.builder().n("-0.00234").build(), AttributeValue.builder()
                                .b(SdkBytes.fromByteArray(Bytes.toBytes("to_be_removed"))).build())
                .build());
        Map<String, AttributeValue> nestedMap2 = new HashMap<>();
        nestedMap2.put("Id", AttributeValue.builder().n("101.22").build());
        nestedMap2.put("Title", AttributeValue.builder().s("Book 10122 Title").build());
        nestedMap2.put("ISBN", AttributeValue.builder().s("111-1111111111999").build());
        nestedMap2.put("InPublication", AttributeValue.builder().bool(true).build());
        nestedMap2.put("NList", AttributeValue.builder()
                .l(AttributeValue.builder().s("NListVal01").build(),
                        AttributeValue.builder().n("-0.00234").build()).build());
        nestedMap1.put("NestedMap2", AttributeValue.builder().m(nestedMap2).build());
        nestedMap1.put("NSet1", AttributeValue.builder()
                .ns("123.45", "9586.7778", "-124", "-6830.5555", "10238", "-48695").build());
        item.put("NestedMap1", AttributeValue.builder().m(nestedMap1).build());
        Map<String, AttributeValue> nestedList1Map1 = new HashMap<>();
        nestedList1Map1.put("Id", AttributeValue.builder().n("9991234.0231").build());
        nestedList1Map1.put("Title", AttributeValue.builder().s("Book 101 Title").build());
        nestedList1Map1.put("ISBN", AttributeValue.builder().s("111-1111111111").build());
        nestedList1Map1.put("InPublication", AttributeValue.builder().bool(false).build());
        nestedList1Map1.put("IdSet", AttributeValue.builder()
                .ns("204850.69703847", "-3945786.4869476", "20576024", "19306873",
                        "486906704836274959").build());
        nestedList1Map1.put("TitleSet1", AttributeValue.builder()
                .ss("Book 1010 Title", "Book 1011 Title", "Book 1111 Title", "Book 1200 Title",
                        "Book 1201 Title").build());
        nestedList1Map1.put("TitleSet2", AttributeValue.builder()
                .ss("Book 1010 Title", "Book 1011 Title", "Book 1111 Title", "Book 1200 Title",
                        "Book 1201 Title").build());
        nestedList1Map1.put("BinaryTitleSet", AttributeValue.builder()
                .bs(SdkBytes.fromByteArray(Bytes.toBytes("Book 1010 Title Binary")),
                        SdkBytes.fromByteArray(Bytes.toBytes("Book 1011 Title Binary")),
                        SdkBytes.fromByteArray(Bytes.toBytes("Book 1111 Title Binary"))).build());
        item.put("NestedList1", AttributeValue.builder()
                .l(AttributeValue.builder().n("-485.34").build(),
                        AttributeValue.builder().s("1234abcd").build(), AttributeValue.builder()
                                .l(AttributeValue.builder().s("xyz0123").build(),
                                        AttributeValue.builder().m(nestedList1Map1).build())
                                .build()).build());
        item.put("NestedList12", AttributeValue.builder()
                .l(AttributeValue.builder().n("-485.34").build(),
                        AttributeValue.builder().s("1234abcd").build(), AttributeValue.builder()
                                .l(AttributeValue.builder().ss("xyz0123").build(),
                                        AttributeValue.builder()
                                                .bs(SdkBytes.fromByteArray(Bytes.toBytes("val01")),
                                                        SdkBytes.fromByteArray(
                                                                Bytes.toBytes("val02")),
                                                        SdkBytes.fromByteArray(
                                                                Bytes.toBytes("val03"))).build())
                                .build()).build());
    }

    private static Map<String, AttributeValue> getItem3() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", AttributeValue.builder().s("str_val_0").build());
        item.put("AddedId", AttributeValue.builder().n("10").build());
        item.put("attr_1", AttributeValue.builder().n("1295.03").build());
        item.put("Id1", AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("ID_101")))
                .build());
        item.put("attr_5", AttributeValue.builder().l(AttributeValue.builder().n("1234").build(),
                AttributeValue.builder().s("str001").build(),
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {0, 1, 2, 3, 4, 5}))
                        .build()).build());
        Map<String, AttributeValue> nMap1 = new HashMap<>();
        nMap1.put("n_attr_0", AttributeValue.builder().s("str_val_0").build());
        nMap1.put("n_attr_1", AttributeValue.builder().n("1295.03123").build());
        String bytesAttributeVal1 = "2048u5nblwjeiWFGTH(4bf930";
        byte[] bytesAttrVal1 = bytesAttributeVal1.getBytes();
        nMap1.put("n_attr_2",
                AttributeValue.builder().b(SdkBytes.fromByteArray(bytesAttrVal1)).build());
        nMap1.put("n_attr_3", AttributeValue.builder().bool(true).build());
        nMap1.put("n_attr_4", AttributeValue.builder().nul(true).build());
        item.put("attr_6", AttributeValue.builder().m(nMap1).build());
        item.put("Id", AttributeValue.builder().s("12345").build());
        item.put("ColorBytes",
                AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("Black"))).build());
        item.put("RelatedItems",
                AttributeValue.builder().ns("1234", "-485.45582904", "123.0948", "0.111").build());
        item.put("Pictures", AttributeValue.builder()
                .ss("123_rear.jpg", "1235@_rear.jpg", "xyz5@_rear.jpg", "xyz_rear.jpg",
                        "123_front.jpg", "xyz_front.jpg").build());
        item.put("PictureBinarySet", AttributeValue.builder()
                .bs(SdkBytes.fromByteArray(Bytes.toBytes("xyz_rear.jpg")),
                        SdkBytes.fromByteArray(Bytes.toBytes("123_front.jpg")),
                        SdkBytes.fromByteArray(Bytes.toBytes("123abc_rear.jpg")),
                        SdkBytes.fromByteArray(Bytes.toBytes("xyzabc_rear.jpg"))).build());
        item.put("Title", AttributeValue.builder().s("Cycle_1234_new").build());
        item.put("ISBN", AttributeValue.builder().s("111-1111111111").build());
        item.put("InPublication", AttributeValue.builder().bool(false).build());
        Map<String, AttributeValue> nestedMap1 = new HashMap<>();
        nestedMap1.put("Id", AttributeValue.builder().n("101.01").build());
        nestedMap1.put("AddedId", AttributeValue.builder().n("10").build());
        nestedMap1.put("ISBN", AttributeValue.builder().s("111-1111111111").build());
        nestedMap1.put("InPublication", AttributeValue.builder().bool(false).build());
        nestedMap1.put("NList1", AttributeValue.builder()
                .l(AttributeValue.builder().ss("Updated_set_01", "Updated_set_02").build(),
                        AttributeValue.builder().n("-0.00234").build()).build());
        nestedMap1.put("ColorList", AttributeValue.builder()
                .l(AttributeValue.builder().s("Black").build(),
                        AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("White")))
                                .build(), AttributeValue.builder().s("Silver").build()).build());
        Map<String, AttributeValue> nestedMap2 = new HashMap<>();
        nestedMap2.put("Id", AttributeValue.builder().n("-12243.78").build());
        nestedMap2.put("NewID", AttributeValue.builder().s("12345").build());
        nestedMap2.put("Title", AttributeValue.builder().s("Book 10122 Title").build());
        nestedMap2.put("ISBN", AttributeValue.builder().s("111-1111111111999").build());
        nestedMap2.put("NList", AttributeValue.builder()
                .l(AttributeValue.builder().n("12.22").build(),
                        AttributeValue.builder().n("-0.00234").build(),
                        AttributeValue.builder().nul(true).build()).build());
        nestedMap1.put("NestedMap2", AttributeValue.builder().m(nestedMap2).build());
        nestedMap1.put("NSet1",
                AttributeValue.builder().ns("123.45", "9586.7778", "-124", "10238").build());
        item.put("NestedMap1", AttributeValue.builder().m(nestedMap1).build());
        Map<String, AttributeValue> nestedList1Map1 = new HashMap<>();
        nestedList1Map1.put("Id", AttributeValue.builder().n("101.01").build());
        nestedList1Map1.put("Title", AttributeValue.builder().s("Book 101 Title").build());
        nestedList1Map1.put("ISBN", AttributeValue.builder().s("111-1111111122").build());
        nestedList1Map1.put("InPublication", AttributeValue.builder().bool(false).build());
        nestedList1Map1.put("IdSet", AttributeValue.builder()
                .ns("204850.697038423", "-9457860.98693947", "20576024", "19306873",
                        "9067048362749590684").build());
        nestedList1Map1.put("TitleSet2",
                AttributeValue.builder().ss("Book 1111 Title", "Book 1200 Title", "Book 1201 Title")
                        .build());
        nestedList1Map1.put("BinaryTitleSet", AttributeValue.builder()
                .bs(SdkBytes.fromByteArray(Bytes.toBytes("Book 1010 Title Binary")),
                        SdkBytes.fromByteArray(Bytes.toBytes("Book 1011 Title Binary")),
                        SdkBytes.fromByteArray(Bytes.toBytes("Book 1111 Title Binary"))).build());
        item.put("NestedList1", AttributeValue.builder()
                .l(AttributeValue.builder().n("-485.34").build(),
                        AttributeValue.builder().s("1234abcd").build(), AttributeValue.builder()
                                .l(AttributeValue.builder().s("xyz0123").build(),
                                        AttributeValue.builder().m(nestedList1Map1).build())
                                .build()).build());
        item.put("NestedList12", AttributeValue.builder()
                .l(AttributeValue.builder().n("-485.34").build(),
                        AttributeValue.builder().s("1234abcd").build(), AttributeValue.builder()
                                .l(AttributeValue.builder().ss("xyz0123", "xyz01234", "abc01234")
                                        .build(), AttributeValue.builder()
                                        .bs(SdkBytes.fromByteArray(Bytes.toBytes("val01")),
                                                SdkBytes.fromByteArray(Bytes.toBytes("val02")),
                                                SdkBytes.fromByteArray(Bytes.toBytes("val03")),
                                                SdkBytes.fromByteArray(Bytes.toBytes("val04")))
                                        .build(), AttributeValue.builder()
                                        .ns("-234.56", "123", "93756.93475960549",
                                                "9375572.6870899").build()).build()).build());
        return item;
    }

    private static Map<String, AttributeValue> getItem4() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", AttributeValue.builder().s("str_val_0").build());
        item.put("AddedId", AttributeValue.builder().n("10").build());
        item.put("attr_1", AttributeValue.builder().n("1295.03").build());
        item.put("Id1", AttributeValue.builder().s("12345").build());
        item.put("attr_5", AttributeValue.builder().l(AttributeValue.builder().n("1224").build(),
                AttributeValue.builder().s("str001").build(),
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {0, 1, 2, 3, 4, 5}))
                        .build()).build());
        Map<String, AttributeValue> nMap1 = new HashMap<>();
        nMap1.put("n_attr_0", AttributeValue.builder().s("str_val_0").build());
        nMap1.put("n_attr_1", AttributeValue.builder().n("1223456.03").build());
        String bytesAttributeVal1 = "2048u5nblwjeiWFGTH(4bf930";
        byte[] bytesAttrVal1 = bytesAttributeVal1.getBytes();
        nMap1.put("n_attr_2",
                AttributeValue.builder().b(SdkBytes.fromByteArray(bytesAttrVal1)).build());
        nMap1.put("n_attr_3", AttributeValue.builder().bool(true).build());
        nMap1.put("n_attr_4", AttributeValue.builder().nul(true).build());
        item.put("attr_6", AttributeValue.builder().m(nMap1).build());
        item.put("Id", AttributeValue.builder().s("12345").build());
        item.put("ColorBytes",
                AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("Black"))).build());
        item.put("RelatedItems",
                AttributeValue.builder().ns("1234", "-485.45582904", "123.0948", "0.111").build());
        item.put("Pictures", AttributeValue.builder()
                .ss("123_rear.jpg", "1235@_rear.jpg", "xyz5@_rear.jpg", "xyz_rear.jpg",
                        "123_front.jpg", "xyz_front.jpg").build());
        item.put("PictureBinarySet", AttributeValue.builder()
                .bs(SdkBytes.fromByteArray(Bytes.toBytes("xyz_rear.jpg")),
                        SdkBytes.fromByteArray(Bytes.toBytes("123_front.jpg")),
                        SdkBytes.fromByteArray(Bytes.toBytes("123abc_rear.jpg")),
                        SdkBytes.fromByteArray(Bytes.toBytes("xyzabc_rear.jpg"))).build());
        item.put("Title", AttributeValue.builder().s("Cycle_1234_new").build());
        item.put("ISBN", AttributeValue.builder().s("111-1111111111").build());
        item.put("InPublication", AttributeValue.builder().bool(false).build());
        Map<String, AttributeValue> nestedMap1 = new HashMap<>();
        nestedMap1.put("Id", AttributeValue.builder().n("101.01").build());
        nestedMap1.put("AddedId", AttributeValue.builder().n("10").build());
        nestedMap1.put("ISBN", AttributeValue.builder().s("111-1111111111").build());
        nestedMap1.put("InPublication", AttributeValue.builder().bool(false).build());
        nestedMap1.put("NList1", AttributeValue.builder()
                .l(AttributeValue.builder().ss("Updated_set_01", "Updated_set_02").build(),
                        AttributeValue.builder().n("-0.00234").build()).build());
        nestedMap1.put("ColorList", AttributeValue.builder()
                .l(AttributeValue.builder().s("Black").build(),
                        AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("White")))
                                .build(), AttributeValue.builder().s("Silver").build()).build());
        Map<String, AttributeValue> nestedMap2 = new HashMap<>();
        nestedMap2.put("Id", AttributeValue.builder().n("-12243.78").build());
        nestedMap2.put("NewID", AttributeValue.builder().s("12345").build());
        nestedMap2.put("Title", AttributeValue.builder().s("Book 10122 Title").build());
        nestedMap2.put("ISBN", AttributeValue.builder().s("111-1111111111999").build());
        nestedMap2.put("NList", AttributeValue.builder()
                .l(AttributeValue.builder().n("12.22").build(),
                        AttributeValue.builder().n("-0.00234").build(),
                        AttributeValue.builder().nul(true).build()).build());
        nestedMap1.put("NestedMap2", AttributeValue.builder().m(nestedMap2).build());
        nestedMap1.put("NSet1", AttributeValue.builder()
                .ns("123.45", "9586.7778", "-124", "10238", "-0.29475029304").build());
        item.put("NestedMap1", AttributeValue.builder().m(nestedMap1).build());
        Map<String, AttributeValue> nestedList1Map1 = new HashMap<>();
        nestedList1Map1.put("Id", AttributeValue.builder().n("101.01").build());
        nestedList1Map1.put("Title", AttributeValue.builder().s("Book 101 Title").build());
        nestedList1Map1.put("ISBN", AttributeValue.builder().s("111-1111111122").build());
        nestedList1Map1.put("InPublication", AttributeValue.builder().bool(false).build());
        nestedList1Map1.put("IdSet", AttributeValue.builder()
                .ns("2048502.69703847", "-45786.4869476", "20576024", "19306873",
                        "83627495940380684").build());
        nestedList1Map1.put("TitleSet2",
                AttributeValue.builder().ss("Book 1111 Title", "Book 1200 Title", "Book 1201 Title")
                        .build());
        nestedList1Map1.put("BinaryTitleSet", AttributeValue.builder()
                .bs(SdkBytes.fromByteArray(Bytes.toBytes("Book 1010 Title Binary")),
                        SdkBytes.fromByteArray(Bytes.toBytes("Book 1011 Title Binary")),
                        SdkBytes.fromByteArray(Bytes.toBytes("Book 1111 Title Binary"))).build());
        item.put("NestedList1", AttributeValue.builder()
                .l(AttributeValue.builder().n("-473.11999999999995").build(),
                        AttributeValue.builder().s("1234abcd").build(), AttributeValue.builder()
                                .l(AttributeValue.builder().s("xyz0123").build(),
                                        AttributeValue.builder().m(nestedList1Map1).build())
                                .build(), AttributeValue.builder().nul(true).build(),
                        AttributeValue.builder().bool(true).build()).build());
        item.put("NestedList12", AttributeValue.builder()
                .l(AttributeValue.builder().n("-485.34").build(),
                        AttributeValue.builder().s("1234abcd").build(), AttributeValue.builder()
                                .l(AttributeValue.builder().ss("xyz0123", "xyz01234", "abc01234")
                                        .build(), AttributeValue.builder()
                                        .bs(SdkBytes.fromByteArray(Bytes.toBytes("val01")),
                                                SdkBytes.fromByteArray(Bytes.toBytes("val02")),
                                                SdkBytes.fromByteArray(Bytes.toBytes("val03")),
                                                SdkBytes.fromByteArray(Bytes.toBytes("val04")))
                                        .build()).build()).build());
        return item;
    }

}

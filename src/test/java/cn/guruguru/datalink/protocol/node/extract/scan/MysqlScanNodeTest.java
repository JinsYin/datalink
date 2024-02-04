package cn.guruguru.datalink.protocol.node.extract.scan;

import cn.guruguru.datalink.protocol.node.Node;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;

public class MysqlScanNodeTest {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    /**
     * Serialization and deserialization testing of the {@link MySqlScanNode}
     */
    @Test
    public void test() throws JsonProcessingException {
        String json =
                "{\"type\":\"MysqlScan\",\"id\":\"N10381712676128\",\"name\":\"N10381712676128\",\"primaryKey\":\"\",\"url\":\"jdbc:mysql://localhost:3306/mydatabase\",\"username\":\"rqyin\",\"password\":\"easipass\",\"tableName\":\"lake_policy\",\"properties\":{\"a\":1},\"fields\":[{\"nodeId\":\"N10381712676128\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"INT\"}}]}";
        // Indirect deserialize to the MySqlScanNode
        MySqlScanNode node1 = (MySqlScanNode) Node.deserialize(json);
        // Directly deserialize to the MySqlScanNode
        MySqlScanNode node2 = objectMapper.readValue(json, MySqlScanNode.class);
        // Serialize
        String newJson = Node.serialize(node2);
        // Deserialize
        MySqlScanNode node3 = objectMapper.readValue(newJson, MySqlScanNode.class);
        // Assertion
        Assert.assertEquals("N10381712676128", node1.getId());
        Assert.assertEquals("MysqlScan", node1.getNodeType());
        Assert.assertEquals(node2.getNodeType(), node1.getNodeType());
        Assert.assertEquals(node2.getNodeType(), node3.getNodeType());
    }
}

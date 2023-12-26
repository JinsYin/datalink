package cn.guruguru.datalink.protocol.node;

import cn.guruguru.datalink.protocol.node.extract.scan.MySqlScanNode;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;

public class NodeTest {
    @Test
    public void deserialize() throws JsonProcessingException {
    String json =
        "{\"type\":\"MysqlScan\",\"id\":\"N10381712676128\",\"name\":\"N10381712676128\",\"primaryKey\":\"\",\"url\":\"jdbc:mysql://localhost:3306/mydatabase\",\"username\":\"rqyin\",\"password\":\"easipass\",\"tableName\":\"lake_policy\",\"properties\":{\"a\":1},\"fields\":[{\"nodeId\":\"N10381712676128\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"INT\"}}]}";
        // Indirect convert to the MySqlScanNode
        MySqlScanNode node1 = (MySqlScanNode) Node.serialize(json);
        // Directly convert to the MySqlScanNode
        MySqlScanNode node2 = new ObjectMapper().readValue(json, MySqlScanNode.class);
        Assert.assertEquals("N10381712676128", node1.getId());
        Assert.assertEquals("MysqlScan", node1.getNodeType());
        Assert.assertEquals(node2.getNodeType(), node1.getNodeType());
    }
}

package cn.guruguru.datalink.protocol.node;

import cn.guruguru.datalink.protocol.node.extract.scan.MySqlScanNode;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import org.junit.Assert;
import org.junit.Test;

public class NodeTest {
    @Test
    public void deserialize() throws JsonProcessingException {
    String json =
        "{\"type\":\"MysqlScan\",\"id\":\"N10381712676128\",\"name\":\"N10381712676128\",\"primaryKey\":\"\",\"url\":\"jdbc:mysql://localhost:3306/mydatabase\",\"username\":\"rqyin\",\"password\":\"easipass\",\"tableName\":\"lake_policy\",\"properties\":{\"a\":1},\"fields\":[{\"nodeId\":\"N10381712676128\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"INT\"}}]}";
        // Automatically convert to the MySqlScanNode
        MySqlScanNode node = (MySqlScanNode) Node.serialize(json);
        Assert.assertEquals("N10381712676128", node.getId());
        Assert.assertEquals("MysqlScan", node.getNodeType());
    }
}

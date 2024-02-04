package cn.guruguru.datalink.protocol.node;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for the {@link Node}
 */
public class NodeTest {

    /**
     * Deserialization testing of the {@link Node}
     */
    @Test
    public void test() throws JsonProcessingException {
    String json =
        "{\"type\":\"MysqlScan\",\"id\":\"N10381712676128\",\"name\":\"N10381712676128\",\"primaryKey\":\"\",\"url\":\"jdbc:mysql://localhost:3306/mydatabase\",\"username\":\"rqyin\",\"password\":\"easipass\",\"tableName\":\"lake_policy\",\"properties\":{\"a\":1},\"fields\":[{\"nodeId\":\"N10381712676128\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"INT\"}}]}";
        // deserialize to the MySqlScanNode
        Node node = Node.deserialize(json);
        // assert
        Assert.assertEquals("N10381712676128", node.getId());
        Assert.assertEquals("N10381712676128", node.getName());
        Assert.assertEquals("MysqlScan", node.getNodeType());
        Assert.assertEquals(1, node.getFields().size());
        Assert.assertEquals(1, node.getProperties().size());
    }
}

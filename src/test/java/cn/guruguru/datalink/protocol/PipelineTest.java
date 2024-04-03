package cn.guruguru.datalink.protocol;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;


import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for the {@link Pipeline}
 */
public class PipelineTest {

  /**
   * Serialization and deserialization testing of the {@link Pipeline}
   */
  @Test
  public void test() throws JsonProcessingException {
        String json =
                "{\"id\":\"L101\",\"name\":\"mysql2lakehouse\",\"description\":\"insert mysql to lakehouse\",\"relation\":{\"fieldRelations\":[],\"nodeRelations\":[{\"type\":\"Map\",\"inputs\":[\"N10381712676128\"],\"outputs\":[\"N10381714539552\"]}]},\"nodes\":[{\"type\":\"MysqlScan\",\"id\":\"N10381712676128\",\"name\":\"N10381712676128\",\"primaryKey\":\"\",\"url\":\"jdbc:mysql://localhost:3306/mydatabase\",\"username\":\"rqyin\",\"password\":\"easipass\",\"tableName\":\"lake_policy\",\"properties\":{\"a\":1},\"fields\":[{\"nodeId\":\"N10381712676128\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"INT\"}}]},{\"id\":\"N10381714539552\",\"name\":\"N10381714539552\",\"type\":\"LakehouseLoad\",\"catalog\":\"p1_catalog1\",\"database\":\"db\",\"table\":\"orders\",\"properties\":{\"b\":2},\"fields\":[{\"nodeId\":\"N10381714539552\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"STRING\"}}],\"fieldRelations\":[{\"type\":\"FieldRelation\",\"inputField\":{\"nodeId\":\"N10381712676128\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"INT\"}},\"outputField\":{\"nodeId\":\"N10381714539552\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"STRING\"}}}]}]}";
        // deserialize
        Pipeline srcLink = Pipeline.deserialize(json);
        // serialize
        String newJson = Pipeline.serialize(srcLink);
        // deserialize
        Pipeline dstLink = Pipeline.deserialize(newJson);
        // assert
        Assert.assertEquals("L101", srcLink.getId());
        Assert.assertEquals("MysqlScan", srcLink.getNodes().get(0).getNodeType());
        Assert.assertEquals(
                dstLink.getNodes().get(0).getNodeType(),
                srcLink.getNodes().get(0).getNodeType());
    }
}

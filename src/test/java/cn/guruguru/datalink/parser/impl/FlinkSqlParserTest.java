package cn.guruguru.datalink.parser.impl;

import cn.guruguru.datalink.parser.ParseResult;
import cn.guruguru.datalink.protocol.LinkInfo;
import cn.guruguru.datalink.utils.SqlUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class FlinkSqlParserTest {

    @Test
    public void parse() throws IOException {
    String json =
        "{\"id\":\"L101\",\"name\":\"mysql2lakehouse\",\"description\":\"insert mysql to lakehouse\",\"relation\":{\"fieldRelations\":[],\"nodeRelations\":[{\"type\":\"Map\",\"inputs\":[\"N10381712676128\"],\"outputs\":[\"N10381714539552\"]}]},\"nodes\":[{\"type\":\"MysqlScan\",\"id\":\"N10381712676128\",\"name\":\"N10381712676128\",\"primaryKey\":\"\",\"url\":\"jdbc:mysql://localhost:3306/mydatabase\",\"username\":\"rqyin\",\"password\":\"easipass\",\"tableName\":\"lake_policy\",\"properties\":{\"a\":1},\"fields\":[{\"nodeId\":\"N10381712676128\",\"type\":\"DataField\",\"name\":\"id\",\"fieldFormat\":{\"type\":\"INT\"}}]},{\"id\":\"N10381714539552\",\"name\":\"N10381714539552\",\"type\":\"LakehouseLoad\",\"catalog\":\"p1_catalog1\",\"database\":\"db\",\"table\":\"orders\",\"properties\":{\"b\":2},\"fields\":[{\"nodeId\":\"N10381714539552\",\"type\":\"DataField\",\"name\":\"id\",\"fieldFormat\":{\"type\":\"STRING\"}}],\"fieldRelations\":[{\"type\":\"FieldRelation\",\"inputField\":{\"nodeId\":\"N10381712676128\",\"type\":\"DataField\",\"name\":\"id\",\"fieldFormat\":{\"type\":\"INT\"}},\"outputField\":{\"nodeId\":\"N10381714539552\",\"type\":\"DataField\",\"name\":\"id\",\"fieldFormat\":{\"type\":\"STRING\"}}}]}]}";
        LinkInfo linkInfo = LinkInfo.serialize(json);
        FlinkSqlParser parser = FlinkSqlParser.getInstance(linkInfo);
        ParseResult parseResult = parser.parse();
        String actual = SqlUtil.compress(parseResult.getSqlScript());
        String expected = "CREATE TABLE IF NOT EXISTS `lake_policy`(`id` INT) WITH (" +
                "'a' = '1', 'connector' = 'jdbc', " +
                "'url' = 'jdbc:mysql://localhost:3306/mydatabase', " +
                "'username' = 'rqyin', " +
                "'password' = 'easipass', " +
                "'table-name' = 'lake_policy');"
                + "CREATE TABLE IF NOT EXISTS `p1_catalog1`.`db`.`orders`(`id` STRING) WITH ('b' = '2');"
                + "INSERT INTO `p1_catalog1`.`db`.`orders` SELECT CAST(`id` as STRING) AS `id` FROM `lake_policy`;";
        Assert.assertEquals(expected, actual);
    }
}

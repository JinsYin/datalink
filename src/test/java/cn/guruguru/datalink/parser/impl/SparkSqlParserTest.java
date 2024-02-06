package cn.guruguru.datalink.parser.impl;

import cn.guruguru.datalink.parser.Parser;
import cn.guruguru.datalink.parser.factory.ParserFactory;
import cn.guruguru.datalink.parser.factory.SparkSqlParserFactory;
import cn.guruguru.datalink.parser.result.ParseResult;
import cn.guruguru.datalink.protocol.LinkInfo;
import cn.guruguru.datalink.utils.SqlUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class SparkSqlParserTest {

    @Test
    public void parseMysqlScan() throws IOException {
        String json =
                "{\"id\":\"L101\",\"name\":\"mysql2lakehouse\",\"description\":\"insert mysql to lakehouse\",\"relation\":{\"fieldRelations\":[],\"nodeRelations\":[{\"type\":\"Map\",\"inputs\":[\"N10381712676128\"],\"outputs\":[\"N10381714539552\"]}]},\"nodes\":[{\"type\":\"MysqlScan\",\"id\":\"N10381712676128\",\"name\":\"N10381712676128\",\"primaryKey\":\"\",\"url\":\"jdbc:mysql://localhost:3306/mydatabase\",\"username\":\"rqyin\",\"password\":\"easipass\",\"tableName\":\"lake_policy\",\"properties\":{\"a\":1},\"fields\":[{\"nodeId\":\"N10381712676128\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"INT\"}}]},{\"id\":\"N10381714539552\",\"name\":\"N10381714539552\",\"type\":\"LakehouseLoad\",\"catalog\":\"p1_catalog1\",\"database\":\"db\",\"table\":\"orders\",\"properties\":{\"b\":2},\"fields\":[{\"nodeId\":\"N10381714539552\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"STRING\"}}],\"fieldRelations\":[{\"type\":\"FieldRelation\",\"inputField\":{\"nodeId\":\"N10381712676128\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"INT\"}},\"outputField\":{\"nodeId\":\"N10381714539552\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"STRING\"}}}]}]}";
        LinkInfo linkInfo = LinkInfo.deserialize(json);
        ParserFactory parserFactory = new SparkSqlParserFactory();
        final Parser sparkSqlParser = parserFactory.createParser();
        ParseResult parseResult = sparkSqlParser.parse(linkInfo);
        String actual = SqlUtil.compress(parseResult.getSqlScript());
        String expected = "CREATE TABLE IF NOT EXISTS `lake_policy`(`id` INT) " +
                          "USING org.apache.spark.sql.jdbc " +
                          "OPTIONS (" +
                          "a \"1\", " +
                          "url \"jdbc:mysql://localhost:3306/mydatabase\", " +
                          "user \"rqyin\", " +
                          "password \"easipass\", " +
                          "dbtable \"lake_policy\");"
                          + "CREATE TEMPORARY VIEW `p1_catalog1`.`db`.`orders`(`id` STRING) USING arctic OPTIONS (b \"2\");"
                          + "INSERT INTO `p1_catalog1`.`db`.`orders` SELECT CAST(`id` as STRING) AS `id` FROM `lake_policy`;";
        Assert.assertEquals(expected, actual);
    }
}

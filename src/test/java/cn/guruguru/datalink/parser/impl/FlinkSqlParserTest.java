package cn.guruguru.datalink.parser.impl;

import cn.guruguru.datalink.parser.result.ParseResult;
import cn.guruguru.datalink.parser.Parser;
import cn.guruguru.datalink.parser.factory.FlinkSqlParserFactory;
import cn.guruguru.datalink.parser.factory.ParserFactory;
import cn.guruguru.datalink.protocol.Pipeline;
import cn.guruguru.datalink.utils.SqlUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class FlinkSqlParserTest {

    @Test
    public void parseMysqlScan() throws IOException {
    String json =
        "{\"id\":\"L101\",\"name\":\"mysql2amoro\",\"description\":\"insert mysql to amoro\",\"relation\":{\"fieldRelations\":[],\"nodeRelations\":[{\"type\":\"Map\",\"inputs\":[\"N10381712676128\"],\"outputs\":[\"N10381714539552\"]}]},\"nodes\":[{\"type\":\"MysqlScan\",\"id\":\"N10381712676128\",\"name\":\"N10381712676128\",\"primaryKey\":\"\",\"url\":\"jdbc:mysql://localhost:3306/mydatabase\",\"username\":\"rqyin\",\"password\":\"easipass\",\"tableName\":\"lake_policy\",\"primaryKey\":\"id\",\"properties\":{\"a\":1},\"fields\":[{\"nodeId\":\"N10381712676128\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"INT\"}}]},{\"id\":\"N10381714539552\",\"name\":\"N10381714539552\",\"type\":\"AmoroLoad\",\"catalog\":\"p1_catalog1\",\"database\":\"db\",\"table\":\"orders\",\"primaryKey\":\"id\",\"properties\":{\"b\":2},\"fields\":[{\"nodeId\":\"N10381714539552\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"STRING\"}}],\"fieldRelations\":[{\"type\":\"FieldRelation\",\"inputField\":{\"nodeId\":\"N10381712676128\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"INT\"}},\"outputField\":{\"nodeId\":\"N10381714539552\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"STRING\"}}}]}]}";
        Pipeline pipeline = Pipeline.deserialize(json);
        ParserFactory parserFactory = new FlinkSqlParserFactory();
        final Parser flinkSqlParser = parserFactory.createParser();
        ParseResult parseResult = flinkSqlParser.parse(pipeline);
        String actual = SqlUtil.compress(parseResult.getSqlScript());
        String expected = "CREATE TABLE IF NOT EXISTS `lake_policy`(`id` INT, PRIMARY KEY (`id`) NOT ENFORCED) WITH (" +
                "'a' = '1', 'connector' = 'jdbc', " +
                "'url' = 'jdbc:mysql://localhost:3306/mydatabase', " +
                "'username' = 'rqyin', " +
                "'password' = 'easipass', " +
                "'table-name' = 'lake_policy');"
                + "CREATE TABLE IF NOT EXISTS `p1_catalog1`.`db`.`orders`(`id` STRING, PRIMARY KEY (`id`) NOT ENFORCED) WITH ('b' = '2');"
                + "INSERT INTO `p1_catalog1`.`db`.`orders` SELECT CAST(`id` as STRING) AS `id` FROM `lake_policy`;";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void parseOracleCdc() throws IOException {
    String json =
        "{\"id\":\"L101\",\"name\":\"oraclecdc2amoro\",\"description\":\"insert oracle-cdc to amoro\",\"relation\":{\"fieldRelations\":[],\"nodeRelations\":[{\"type\":\"Map\",\"inputs\":[\"N10381712676128\"],\"outputs\":[\"N10381714539552\"]}]},\"nodes\":[{\"type\":\"OracleCdc\",\"id\":\"N10381712676128\",\"name\":\"N10381712676128\",\"primaryKey\":\"\",\"hostname\":\"localhost\",\"port\":3306,\"username\":\"rqyin\",\"password\":\"easipass\",\"databaseName\":\"test12c\",\"schemaName\":\"s1\",\"tableName\":\"lake_policy\",\"properties\":{\"a\":1},\"fields\":[{\"nodeId\":\"N10381712676128\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"NUMBER\",\"precision\":38,\"scale\":0}}]},{\"id\":\"N10381714539552\",\"name\":\"N10381714539552\",\"type\":\"AmoroLoad\",\"catalog\":\"p1_catalog1\",\"database\":\"db\",\"table\":\"orders\",\"properties\":{\"b\":2},\"fields\":[{\"nodeId\":\"N10381714539552\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"DECIMAL\",\"precision\":38,\"scale\":0}}],\"fieldRelations\":[{\"type\":\"FieldRelation\",\"inputField\":{\"nodeId\":\"N10381712676128\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"NUMBER\",\"precision\":38,\"scale\":0}},\"outputField\":{\"nodeId\":\"N10381714539552\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"DECIMAL\",\"precision\":38,\"scale\":0}}}]}]}";
        Pipeline pipeline = Pipeline.deserialize(json);
        ParserFactory parserFactory = new FlinkSqlParserFactory();
        final Parser flinkSqlParser = parserFactory.createParser();
        ParseResult parseResult = flinkSqlParser.parse(pipeline);
        String actual = SqlUtil.compress(parseResult.getSqlScript());
        String expected = "CREATE TABLE IF NOT EXISTS `lake_policy`(`id` DECIMAL(38, 0)) WITH (" +
                "'a' = '1', 'connector' = 'oracle-cdc', " +
                "'hostname' = 'localhost', " +
                "'port' = '3306', " +
                "'username' = 'rqyin', " +
                "'password' = 'easipass', " +
                "'database-name' = 'test12c', " +
                "'schema-name' = 's1', " +
                "'table-name' = 'lake_policy');"
                + "CREATE TABLE IF NOT EXISTS `p1_catalog1`.`db`.`orders`(`id` DECIMAL(38, 0)) WITH ('b' = '2');"
                + "INSERT INTO `p1_catalog1`.`db`.`orders` SELECT CAST(`id` as DECIMAL(38, 0)) AS `id` FROM `lake_policy`;";
        Assert.assertEquals(expected, actual);
    }
}

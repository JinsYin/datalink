package cn.guruguru.datalink.parser.impl;

import cn.guruguru.datalink.parser.Parser;
import cn.guruguru.datalink.parser.factory.ParserFactory;
import cn.guruguru.datalink.parser.factory.SparkSqlParserFactory;
import cn.guruguru.datalink.parser.result.ParseResult;
import cn.guruguru.datalink.protocol.Pipeline;
import cn.guruguru.datalink.utils.SqlUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class SparkSqlParserTest {

    @Test
    public void parseMysqlScan() throws IOException {
        String json =
                "{\"id\":\"L101\",\"name\":\"mysql2amoro\",\"description\":\"insert mysql to amoro\",\"relation\":{\"fieldRelations\":[],\"nodeRelations\":[{\"type\":\"Map\",\"inputs\":[\"N10381712676128\"],\"outputs\":[\"N10381714539552\"]}]},\"nodes\":[{\"type\":\"MysqlScan\",\"id\":\"N10381712676128\",\"name\":\"N10381712676128\",\"primaryKey\":\"\",\"url\":\"jdbc:mysql://localhost:3306/mydatabase\",\"username\":\"rqyin\",\"password\":\"easipass\",\"tableName\":\"lake_policy\",\"primaryKey\":\"id\",\"properties\":{\"a\":1},\"fields\":[{\"nodeId\":\"N10381712676128\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"INT\"}}]},{\"id\":\"N10381714539552\",\"name\":\"N10381714539552\",\"type\":\"AmoroLoad\",\"catalog\":\"p1_catalog1\",\"database\":\"db\",\"table\":\"orders\",\"primaryKey\":\"id\",\"properties\":{\"b\":2},\"fields\":[{\"nodeId\":\"N10381714539552\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"STRING\"}}],\"fieldRelations\":[{\"type\":\"FieldRelation\",\"inputField\":{\"nodeId\":\"N10381712676128\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"INT\"}},\"outputField\":{\"nodeId\":\"N10381714539552\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"STRING\"}}}]}]}";
        Pipeline pipeline = Pipeline.deserialize(json);
        ParserFactory parserFactory = new SparkSqlParserFactory();
        final Parser sparkSqlParser = parserFactory.createParser();
        ParseResult parseResult = sparkSqlParser.parse(pipeline);
        String actual = SqlUtil.compress(parseResult.getSqlScript());
        String expected = "CREATE OR REPLACE TEMPORARY VIEW `lake_policy` " +
                          "USING org.apache.spark.sql.jdbc " +
                          "OPTIONS (" +
                          "a '1', " +
                          "url 'jdbc:mysql://localhost:3306/mydatabase', " +
                          "user 'rqyin', " +
                          "password 'easipass', " +
                          "dbtable 'lake_policy');"
                          + "CREATE TABLE IF NOT EXISTS `p1_catalog1`.`db`.`orders`(`id` STRING, PRIMARY KEY (`id`)) USING arctic OPTIONS (b '2');"
                          + "INSERT INTO `p1_catalog1`.`db`.`orders` SELECT COALESCE(CAST(`id` as STRING), \"\") AS `id` FROM `lake_policy`;";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void parseOracleScan() throws IOException {
        String json =
                "{\"id\":\"L101\",\"name\":\"oracle2amoro\",\"description\":\"insert oracle to amoro\",\"relation\":{\"fieldRelations\":[],\"nodeRelations\":[{\"type\":\"Map\",\"inputs\":[\"N10381712676128\"],\"outputs\":[\"N10381714539552\"]}]},\"nodes\":[{\"type\":\"OracleScan\",\"id\":\"N10381712676128\",\"name\":\"N10381712676128\",\"primaryKey\":\"id\",\"url\":\"jdbc:oracle:thin:@//localhost:1521/testdb\",\"username\":\"rqyin\",\"password\":\"easipass\",\"tableName\":\"MYSCHEMA.my_table\",\"properties\":{\"a\":1},\"fields\":[{\"nodeId\":\"N10381712676128\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"INT\"}}]},{\"id\":\"N10381714539552\",\"name\":\"N10381714539552\",\"type\":\"AmoroLoad\",\"catalog\":\"p1_catalog1\",\"database\":\"db\",\"table\":\"orders\",\"primaryKey\":\"id\",\"properties\":{\"b\":2},\"fields\":[{\"nodeId\":\"N10381714539552\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"INT\"}}],\"fieldRelations\":[{\"type\":\"FieldRelation\",\"inputField\":{\"nodeId\":\"N10381712676128\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"INT\"}},\"outputField\":{\"nodeId\":\"N10381714539552\",\"type\":\"DataField\",\"name\":\"id\",\"dataType\":{\"type\":\"INT\"}}}]}]}";
        Pipeline pipeline = Pipeline.deserialize(json);
        ParserFactory parserFactory = new SparkSqlParserFactory();
        final Parser sparkSqlParser = parserFactory.createParser();
        ParseResult parseResult = sparkSqlParser.parse(pipeline);
        String actual = SqlUtil.compress(parseResult.getSqlScript());
        String expected = "CREATE OR REPLACE TEMPORARY VIEW `MYSCHEMA.my_table` " +
                "USING org.apache.spark.sql.jdbc " +
                "OPTIONS (" +
                "a '1', " +
                "url 'jdbc:oracle:thin:@//localhost:1521/testdb', " +
                "user 'rqyin', " +
                "password 'easipass', " +
                "dbtable 'MYSCHEMA.\"my_table\"');"
                + "CREATE TABLE IF NOT EXISTS `p1_catalog1`.`db`.`orders`(`id` INT, PRIMARY KEY (`id`)) USING arctic OPTIONS (b '2');"
                + "INSERT INTO `p1_catalog1`.`db`.`orders` SELECT COALESCE(CAST(`id` as INT), 0) AS `id` FROM `MYSCHEMA.my_table`;";
        Assert.assertEquals(expected, actual);
    }
}

package cn.guruguru.datalink.converter.sql;

import cn.guruguru.datalink.converter.enums.DDLDialect;
import cn.guruguru.datalink.converter.sql.result.FlinkSqlConverterResult;
import cn.guruguru.datalink.converter.table.TableField;
import cn.guruguru.datalink.converter.table.TableSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class FlinkSqlConverterTest {

    private static final FlinkSqlConverter flinkSqlConverter = new FlinkSqlConverter();

    @Test
    public void testConvertSingleStatementForOracle() {
        String createSQL =
            "CREATE TABLE \"ADM_BDPP\".\"PARAMSYS\" \n"
                + "   (\t\"PARAM_SEQUENCE\" NUMBER(15,0) NOT NULL ENABLE, \n"
                + "  TASKNAME VARCHAR2(50) NOT NULL DEFAULT '123', \n"
                + "\t\"TASKINTERVAL\" VARCHAR2(10 CHAR) NOT NULL DEFAULT 0\n"
                + "   )";
        List<FlinkSqlConverterResult> actualResults = flinkSqlConverter.toEngineDDL(
                DDLDialect.Oracle, "P1_CATALOG1", "DB1", createSQL);
        String actualDDL = actualResults.get(0).getConverterResult();
        String expectedDDL =
            "CREATE TABLE `P1_CATALOG1`.`ADM_BDPP`.`PARAMSYS` (\n"
                + "    `PARAM_SEQUENCE` DECIMAL(15, 0) NOT NULL,\n"
                + "    `TASKNAME` STRING NOT NULL,\n"
                + "    `TASKINTERVAL` STRING NOT NULL\n"
                + ");";
        System.out.println(actualDDL);
        Assert.assertEquals(expectedDDL, actualDDL);
    }

    @Test
    public void testConvertMultiStatementsForOracle() {
        String sqls = "-- 123\n" +
                "CREATE TABLE \"API_OPER\".\"EDG25_APP_MESSAGE\" \n" +
                "   (    \"ID\" VARCHAR2(32 CHAR) NOT NULL ENABLE, -- abc\n" +
                "    \"AID\" VARCHAR2(32 CHAR), \n" +
                "    \"INFO\" VARCHAR2(2048 CHAR), \n" +
                "     PRIMARY KEY (\"ID\")\n" +
                "  USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS \n" +
                "  TABLESPACE \"SRC_DATA\"  ENABLE\n" +
                "   ) SEGMENT CREATION DEFERRED \n" +
                "  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 \n" +
                " NOCOMPRESS LOGGING\n" +
                "  TABLESPACE \"SRC_DATA\" ;\n" +
                "\n" +
                "COMMENT ON COLUMN \"API_OPER\".\"EDG25_APP_MESSAGE\".\"ID\" IS '主键';\n" +
                "   COMMENT ON COLUMN \"API_OPER\".\"EDG25_APP_MESSAGE\".\"AID\" IS 'appList主键';\n" +
                "   COMMENT ON COLUMN \"API_OPER\".\"EDG25_APP_MESSAGE\".\"INFO\" IS '发送日期';";

        List<FlinkSqlConverterResult> actualResults = flinkSqlConverter.toEngineDDL(
                DDLDialect.Oracle, "P1_CATALOG1", "DB1", sqls);
        String actualDDL = actualResults.get(0).getConverterResult();
        String expectedDDL =
                "CREATE TABLE `P1_CATALOG1`.`API_OPER`.`EDG25_APP_MESSAGE` (\n"
                        + "    `ID` STRING NOT NULL COMMENT '主键',\n"
                        + "    `AID` STRING COMMENT 'appList主键',\n"
                        + "    `INFO` STRING COMMENT '发送日期'\n"
                        + ");";
        System.out.println(actualDDL);
        Assert.assertEquals(expectedDDL, actualDDL);
    }

    @Test
    public void testConvertTableSchema() {
        TableField idColumn = new TableField("ID", "VARCHAR2", null, null, "主键");
        TableField aidColumn = new TableField("AID", "VARCHAR2", null, null, null);
        TableField infoColumn = new TableField("INFO", "VARCHAR2", null, null, "发送日期");
        List<TableField> columns = Arrays.asList(idColumn, aidColumn, infoColumn);
        TableSchema tableSchema = TableSchema.builder()
                .tableIdentifier("`P1_CATALOG1`.`API_OPER`.`EDG25_APP_MESSAGE`")
                .columns(columns)
                .tableComment("Test Table")
                .build();
        List<TableSchema> tableSchemas = Collections.singletonList(tableSchema);
        List<FlinkSqlConverterResult> results = flinkSqlConverter.toEngineDDL(DDLDialect.Oracle, tableSchemas);
        String actualDDL = results.stream().map(FlinkSqlConverterResult::getConverterResult).collect(Collectors.joining());
        String expectedDDL =
            "CREATE TABLE `P1_CATALOG1`.`API_OPER`.`EDG25_APP_MESSAGE` \n"
                + "    `ID` STRING '主键',\n"
                + "    `AID` STRING,\n"
                + "    `INFO` STRING '发送日期'\n"
                + ") COMMENT 'Test Table';\n";
        System.out.println(actualDDL);
        Assert.assertEquals(expectedDDL, actualDDL);
    }
}

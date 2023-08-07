package cn.guruguru.datalink.converter.sql;

import org.junit.Assert;
import org.junit.Test;

public class FlinkSqlConverterTest {

    @Test
    public void testConvertToEngineDDL() {
    String createSQL =
        "CREATE TABLE \"ADM_BDPP\".\"PARAMSYS\" \n"
            + "   (\t\"PARAM_SEQUENCE\" NUMBER(15,0) NOT NULL ENABLE, \n"
            + "\t\"TASKNAME\" VARCHAR2(50) NOT NULL DEFAULT '123', \n"
            + "\t\"TASKINTERVAL\" VARCHAR2(10) NOT NULL DEFAULT 0\n"
            + "   )";
        FlinkSqlConverter flinkSqlConverter = new FlinkSqlConverter();
        String actual = flinkSqlConverter.toEngineDDL("Oracle", "p1_catalog1", "db1", createSQL);
        String expected =
            "CREATE TABLE ADM_BDPP.SPARK_PARAM (\n"
                + "    ID DECIMAL(20, 0) NOT NULL,\n"
                + "    APP_NAME STRING,\n"
                + "    SRC_ID DECIMAL(20, 0),\n"
                + "    TGT_ID DECIMAL(20, 0),\n"
                + "    APP_FUNC STRING,\n"
                + "    CONTENT BYTES,\n"
                + "    LASTUPDATEDDT TIMESTAMP(6)\n"
                + ")";
        System.out.println(actual);
        Assert.assertEquals(expected, actual);
    }
}

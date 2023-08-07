package cn.guruguru.datalink.converter.sql;

import org.junit.Assert;
import org.junit.Test;

public class FlinkSqlConverterTest {

    @Test
    public void testConvertToEngineDDL() {
        String createSQL =
            "CREATE TABLE ADM_BDPP.SPARK_PARAM ("
                + "ID NUMBER(20,0) NOT NULL, "
                + "APP_NAME VARCHAR(80), "
                + "SRC_ID NUMBER(20,0), "
                + "TGT_ID NUMBER(20,0), "
                + "APP_FUNC VARCHAR2(6),"
                + "CONTENT BLOB,"
                + "LASTUPDATEDDT TIMESTAMP(6)"
                + ");";
        FlinkSqlConverter flinkSqlConverter = new FlinkSqlConverter();
        String actual = flinkSqlConverter.toEngineDDL("Oracle", createSQL);
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

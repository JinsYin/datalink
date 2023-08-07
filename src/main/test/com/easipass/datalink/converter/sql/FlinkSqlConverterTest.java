package cn.guruguru.datalink.converter.sql;

import cn.guruguru.datalink.converter.enums.DDLDialect;
import cn.guruguru.datalink.converter.sql.result.FlinkSqlConverterResult;
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
    FlinkSqlConverterResult actualResult =
        flinkSqlConverter.toEngineDDL(DDLDialect.Oracle, "P1_CATALOG1", "DB1", createSQL);
        String actualDDL = actualResult.getDdl();
        String expectedDDL =
            "CREATE TABLE `P1_CATALOG1`.`ADM_BDPP`.`PARAMSYS` (\n"
                + "    \"PARAM_SEQUENCE\" DECIMAL(15, 0) NOT NULL,\n"
                + "    \"TASKNAME\" STRING NOT NULL,\n"
                + "    \"TASKINTERVAL\" STRING NOT NULL\n"
                + ")";
        System.out.println(actualDDL);
        Assert.assertEquals(expectedDDL, actualDDL);
    }
}

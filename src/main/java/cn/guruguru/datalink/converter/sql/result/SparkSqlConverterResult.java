package cn.guruguru.datalink.converter.sql.result;

import cn.guruguru.datalink.converter.result.SqlConverterResult;
import cn.guruguru.datalink.converter.table.JdbcDialect;
import cn.guruguru.datalink.converter.statement.CreateDatabaseStatement;
import cn.guruguru.datalink.converter.statement.CreateTableStatement;
import lombok.Data;

import java.util.List;

@Data
public class SparkSqlConverterResult implements SqlConverterResult {
    @Override
    public JdbcDialect getDialect() {
        return null;
    }

    @Override
    public List<CreateDatabaseStatement> getCreateDatabaseStmts() {
        return null;
    }

    @Override
    public List<CreateTableStatement> getCreateTableStmts() {
        return null;
    }
}

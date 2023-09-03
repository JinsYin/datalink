package cn.guruguru.datalink.ddl.result;

import cn.guruguru.datalink.ddl.table.JdbcDialect;
import cn.guruguru.datalink.ddl.statement.CreateDatabaseStatement;
import cn.guruguru.datalink.ddl.statement.CreateTableStatement;
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

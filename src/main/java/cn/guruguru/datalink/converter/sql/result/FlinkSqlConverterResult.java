package cn.guruguru.datalink.converter.sql.result;

import cn.guruguru.datalink.converter.result.SqlConverterResult;
import cn.guruguru.datalink.converter.table.JdbcDialect;
import cn.guruguru.datalink.converter.statement.CreateDatabaseStatement;
import cn.guruguru.datalink.converter.statement.CreateTableStatement;
import com.google.common.base.Preconditions;
import lombok.Data;

import java.util.List;

@Data
public class FlinkSqlConverterResult implements SqlConverterResult {
    private JdbcDialect dialect;
    private List<CreateDatabaseStatement> createDatabaseStatements;
    private List<CreateTableStatement> createTableStatements;

    public FlinkSqlConverterResult(JdbcDialect dialect,
                                   List<CreateDatabaseStatement> createDatabaseStatements,
                                   List<CreateTableStatement> createTableStatements) {
        this.dialect = Preconditions.checkNotNull(dialect, "dialect is null");
        this.createDatabaseStatements = Preconditions.checkNotNull(createDatabaseStatements,
                "createDatabaseStatements is null");
        Preconditions.checkState(!createDatabaseStatements.isEmpty(), "createDatabaseStatements is empty");
        this.createTableStatements = Preconditions.checkNotNull(createTableStatements, "createTableStatements is null");
        Preconditions.checkState(!createTableStatements.isEmpty(), "createTableStatements is empty");
    }

    @Override
    public JdbcDialect getDialect() {
        return this.dialect;
    }

    @Override
    public List<CreateDatabaseStatement> getCreateDatabaseStmts() {
        return this.createDatabaseStatements;
    }

    @Override
    public List<CreateTableStatement> getCreateTableStmts() {
        return this.createTableStatements;
    }
}

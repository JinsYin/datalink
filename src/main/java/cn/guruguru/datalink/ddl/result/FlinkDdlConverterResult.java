package cn.guruguru.datalink.ddl.result;

import cn.guruguru.datalink.ddl.table.JdbcDialect;
import cn.guruguru.datalink.ddl.statement.CreateDatabaseStatement;
import cn.guruguru.datalink.ddl.statement.CreateTableStatement;
import com.google.common.base.Preconditions;
import lombok.Data;

import java.util.List;

@Data
public class FlinkDdlConverterResult implements DdlConverterResult {
    private JdbcDialect dialect;
    private List<CreateDatabaseStatement> createDatabaseStatements;
    private List<CreateTableStatement> createTableStatements;

    public FlinkDdlConverterResult(JdbcDialect dialect,
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

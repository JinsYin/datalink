package cn.guruguru.datalink.ddl.result;

import cn.guruguru.datalink.ddl.table.JdbcDialect;
import cn.guruguru.datalink.ddl.statement.CreateDatabaseStatement;
import cn.guruguru.datalink.ddl.statement.CreateTableStatement;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

public interface DdlConverterResult {

    /**
     * Get JDBC dialect
     *
     * @return JDBC dialect
     */
    JdbcDialect getDialect();

    /**
     * Get CREATE-DATABASE statements. Before creating a table, it is necessary to ensure that the database has been
     * created. The number of CREATE-DATABASE statements is less than or equals to the number of CREATE-TABLE statements
     *
     * @return CREATE-DATABASE statements
     */
    List<CreateDatabaseStatement> getCreateDatabaseStmts();

    /**
     * Get CREATE-TABLE statements
     *
     * @return CREATE-TABLE statements
     */
    List<CreateTableStatement> getCreateTableStmts();

    /**
     * Get DDL SQL
     *
     * @return DDL SQL
     */
    default String getSql() {
        List<String> sqlList = new ArrayList<>();
        getCreateDatabaseStmts().forEach(stmt -> sqlList.add(stmt.getDdl()));
        getCreateTableStmts().forEach(stmt -> sqlList.add(stmt.getDdl()));
        Preconditions.checkState(!sqlList.isEmpty(), "No CREATE-TABLE statements");
        return String.join(";\n", sqlList);
    }
}

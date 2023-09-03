package cn.guruguru.datalink.converter.sql;

import cn.guruguru.datalink.converter.SqlConverter;
import cn.guruguru.datalink.converter.table.JdbcDialect;
import cn.guruguru.datalink.converter.table.CaseStrategy;
import cn.guruguru.datalink.converter.table.DatabaseTableAffix;
import cn.guruguru.datalink.converter.table.TableDuplicateStrategy;
import cn.guruguru.datalink.converter.table.TableSchema;
import cn.guruguru.datalink.exception.UnsupportedEngineException;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

public class SparkSqlConverter implements SqlConverter<List<String>> {

    // ~ converter for table schemas --------------------------------------

    @Override
    public List<String> convertSchemas(JdbcDialect dialect,
                                       List<TableSchema> tableSchemas,
                                       DatabaseTableAffix databaseAffix,
                                       DatabaseTableAffix tableAffix,
                                       TableDuplicateStrategy tableDuplicateStrategy,
                                       CaseStrategy caseStrategy) throws RuntimeException {
        throw new UnsupportedEngineException("Spark engine not supported");
    }

    // ~ converter for sql ------------------------------------------------

    @Override
    public List<String> convertSql(JdbcDialect dialect,
                                   String catalog,
                                   @Nullable String database,
                                   String sql,
                                   CaseStrategy caseStrategy)
            throws RuntimeException  {
        SqlParser.Config sqlParserConfig = SqlParser.Config.DEFAULT
                .withLex(Lex.ORACLE).withConformance(SqlConformanceEnum.ORACLE_12)
                .withParserFactory(SqlDdlParserImpl.FACTORY);

        SqlParser parser = SqlParser.create(sql, sqlParserConfig);
        SqlNode sqlNode;
        try {
            sqlNode = parser.parseStmt();
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
        return Arrays.asList(convertToSparkDDL(sqlNode));
    }

    private static String convertToSparkDDL(SqlNode sqlNode) {
        SqlDialect sparkDialect = SqlDialect.DatabaseProduct.SPARK.getDialect();
        SqlPrettyWriter prettyWriter = new SqlPrettyWriter(sparkDialect);
        sqlNode.unparse(prettyWriter, 0, 0);
        return prettyWriter.toSqlString().getSql();
    }
}

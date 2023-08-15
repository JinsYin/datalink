package cn.guruguru.datalink.converter.sql;

import cn.guruguru.datalink.converter.SqlConverter;
import cn.guruguru.datalink.converter.enums.DDLDialect;
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

public class SparkSqlConverter implements SqlConverter<String> {

    @Override
    public List<String> toEngineDDL(DDLDialect dialect, String catalog, @Nullable String database, String sqls)
            throws RuntimeException  {
        SqlParser.Config sqlParserConfig = SqlParser.Config.DEFAULT
                .withLex(Lex.ORACLE).withConformance(SqlConformanceEnum.ORACLE_12)
                .withParserFactory(SqlDdlParserImpl.FACTORY);

        SqlParser parser = SqlParser.create(sqls, sqlParserConfig);
        SqlNode sqlNode;
        try {
            sqlNode = parser.parseStmt();
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
        return Arrays.asList(convertToSparkDDL(sqlNode));
    }

    @Override
    public List<String> toEngineDDL(DDLDialect dialect, List<TableSchema> tableSchemas) throws RuntimeException {
        throw new UnsupportedEngineException("Spark engine not supported");
    }

    private static String convertToSparkDDL(SqlNode sqlNode) {
        SqlDialect sparkDialect = SqlDialect.DatabaseProduct.SPARK.getDialect();
        SqlPrettyWriter prettyWriter = new SqlPrettyWriter(sparkDialect);
        sqlNode.unparse(prettyWriter, 0, 0);
        return prettyWriter.toSqlString().getSql();
    }
}

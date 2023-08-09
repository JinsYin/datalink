package cn.guruguru.datalink.converter.sql;

import cn.guruguru.datalink.converter.SqlConverter;
import cn.guruguru.datalink.converter.enums.DDLDialect;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import javax.annotation.Nullable;

public class SparkSqlConverter implements SqlConverter<String> {

    @Override
    public String toEngineDDL(DDLDialect dialect, String catalog, @Nullable String database, String ddl)
            throws RuntimeException  {
        SqlParser.Config sqlParserConfig = SqlParser.Config.DEFAULT
                .withLex(Lex.ORACLE).withConformance(SqlConformanceEnum.ORACLE_12)
                .withParserFactory(SqlDdlParserImpl.FACTORY);

        SqlParser parser = SqlParser.create(ddl, sqlParserConfig);
        SqlNode sqlNode;
        try {
            sqlNode = parser.parseStmt();
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
        return convertToSparkDDL(sqlNode);
    }

    private static String convertToSparkDDL(SqlNode sqlNode) {
        SqlDialect sparkDialect = SqlDialect.DatabaseProduct.SPARK.getDialect();
        SqlPrettyWriter prettyWriter = new SqlPrettyWriter(sparkDialect);
        sqlNode.unparse(prettyWriter, 0, 0);
        return prettyWriter.toSqlString().getSql();
    }
}

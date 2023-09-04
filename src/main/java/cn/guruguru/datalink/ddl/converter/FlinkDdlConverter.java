package cn.guruguru.datalink.ddl.converter;

import cn.guruguru.datalink.ddl.statement.CreateDatabaseStatement;
import cn.guruguru.datalink.ddl.statement.CreateTableStatement;
import cn.guruguru.datalink.ddl.table.JdbcDialect;
import cn.guruguru.datalink.ddl.result.FlinkDdlConverterResult;
import cn.guruguru.datalink.ddl.table.CaseStrategy;
import cn.guruguru.datalink.ddl.table.Affix;
import cn.guruguru.datalink.ddl.table.TableDuplicateStrategy;
import cn.guruguru.datalink.ddl.table.TableField;
import cn.guruguru.datalink.ddl.table.TableSchema;
import cn.guruguru.datalink.ddl.type.FlinkDataTypeConverter;
import cn.guruguru.datalink.exception.IllegalDDLException;
import cn.guruguru.datalink.exception.SQLSyntaxException;
import cn.guruguru.datalink.protocol.field.FieldFormat;
import cn.guruguru.datalink.utils.SqlUtil;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.comment.Comment;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class FlinkDdlConverter implements DdlConverter<FlinkDdlConverterResult> {

    private static final FlinkDataTypeConverter flinkTypeConverter = new FlinkDataTypeConverter();

    // ~ converter for table schemas --------------------------------------

    @Override
    public FlinkDdlConverterResult convertSchema(JdbcDialect dialect,
                                                 List<TableSchema> tableSchemas,
                                                 Affix databaseAffix,
                                                 Affix tableAffix,
                                                 TableDuplicateStrategy tableDuplicateStrategy,
                                                 CaseStrategy caseStrategy) throws RuntimeException {
        Preconditions.checkNotNull(dialect,"dialect is null");
        Preconditions.checkNotNull(tableSchemas,"table schema list is null");
        Preconditions.checkState(!tableSchemas.isEmpty(),"table schema list is empty");
        log.info("start parse {} table schemas", dialect);

        // There may be same databases or tables
        Map<String, String> createDatabaseSqlMap = new LinkedHashMap<>();
        Map<String, String> createTableSqlMap = new LinkedHashMap<>();
        for (TableSchema tableSchema : tableSchemas) {
            // generate CREATE-DATABASE sql
            String catalog = Preconditions.checkNotNull(tableSchema.getCatalog(), "catalog is null");
            String database = Preconditions.checkNotNull(tableSchema.getDatabase(), "database is null");
            String databaseIdentifier = formatDatabaseIdentifier(catalog, database, databaseAffix, caseStrategy);
            String createDatabaseSql = String.format("CREATE DATABASE IF NOT EXISTS %s", databaseIdentifier);
            createDatabaseSqlMap.put(databaseIdentifier, createDatabaseSql);
            // generate CREATE-TABLE sql
            String tableName = Preconditions.checkNotNull(tableSchema.getTableName(), "tableName is null");
            String tableIdentifier = formatTableIdentifier(
                    catalog, database, tableName, databaseAffix, tableAffix, caseStrategy);
            String createTableSql = genCreateTableSqlForSchema(dialect, tableIdentifier, tableSchema, caseStrategy);
            createTableSqlMap.put(tableIdentifier, createTableSql);
        }
        log.info("end parse {} table schemas", dialect);
        return getFlinkSqlConverterResult(dialect, createDatabaseSqlMap, createTableSqlMap);
    }

    /**
     * Generate a CREATE-TABLE sql for a table schema
     *
     * @param dialect JDBC dialect
     * @param tableIdentifier table identifier
     * @param tableSchema table schema
     * @param caseStrategy case strategy
     * @return a CREATE-TABLE sql
     */
    private String genCreateTableSqlForSchema(JdbcDialect dialect,
                                              String tableIdentifier,
                                              TableSchema tableSchema,
                                              CaseStrategy caseStrategy) {
        String tableComment = tableSchema.getTableComment();
        List<TableField> tableFields = tableSchema.getFields();
        Preconditions.checkNotNull(tableFields,"table columns is null");
        Preconditions.checkState(!tableFields.isEmpty(),"table columns is empty");
        StringBuilder createTableSql = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        createTableSql.append(tableIdentifier).append(" (\n");
        for (TableField tableField : tableFields) {
            String columnName = Preconditions.checkNotNull(tableField.getName(),"column name is null");
            String columnType = Preconditions.checkNotNull(tableField.getType(),"column type is null");
            String columnComment = tableField.getComment();
            Integer precision = tableField.getPrecision();
            Integer scale = tableField.getScale();
            FieldFormat fieldFormat = new FieldFormat(columnType, precision, scale);
            String engineFieldType = flinkTypeConverter.toEngineType(dialect.getNodeType(), fieldFormat);
            createTableSql.append("    ").append(formatColumn(columnName, caseStrategy)).append(" ").append(engineFieldType);
            if (!StringUtils.isBlank(columnComment)) {
                createTableSql.append(" COMMENT '").append(columnComment).append("'");
            }
            createTableSql.append(",\n");
        }
        createTableSql.deleteCharAt(createTableSql.length() - 2).append(")");
        if (!StringUtils.isBlank(tableComment)) {
            createTableSql.append(" COMMENT '").append(tableComment).append("'");
        }
        return createTableSql.toString();
    }

    /**
     * Get converter result
     *
     * @param dialect JDBC dialect
     * @param createDatabaseSqlMap map for create database sql
     * @param createTableSqlMap map for create table sql
     * @return {@link FlinkDdlConverterResult}
     */
    private FlinkDdlConverterResult getFlinkSqlConverterResult(JdbcDialect dialect,
                                                               Map<String, String> createDatabaseSqlMap,
                                                               Map<String, String> createTableSqlMap) {
        List<CreateDatabaseStatement> createDatabaseStatements = new ArrayList<>();
        List<CreateTableStatement> createTableStatements = new ArrayList<>();
        createDatabaseSqlMap.forEach((k, v) -> createDatabaseStatements.add(new CreateDatabaseStatement(k, v)));
        createTableSqlMap.forEach((k, v) -> createTableStatements.add(new CreateTableStatement(k, v)));
        return new FlinkDdlConverterResult(dialect, createDatabaseStatements, createTableStatements);
    }

    // ~ converter for sql ------------------------------------------------

    /**
     * Convert to SQL
     *
     * TODO: parse primary key
     * @see <a href="https://techieshouts.com/home/parsing-sql-create-query-using-jsql-parser/">Parsing SQL CREATE query using JSQLParser</a>
     * @param dialect data source type
     * @param targetCatalog target catalog
     * @param defaultDatabase default database, If database is set in SQL (like {@code CREATE TABLE `db1`.`tb1` (...)}),
     *                        it will be ignored
     * @param sql one or more SQL statements from Data Source, non-CREATE-TABLE statements will be ignored
     */
    @Override
    public FlinkDdlConverterResult convertSql(JdbcDialect dialect,
                                              String targetCatalog,
                                              @Nullable String defaultDatabase,
                                              String sql,
                                              CaseStrategy caseStrategy) throws RuntimeException {
        Preconditions.checkNotNull(dialect,"dialect is null");
        Preconditions.checkNotNull(targetCatalog,"catalog is null");
        Preconditions.checkNotNull(sql,"sql is null");
        log.info("start parse {} SQL:{}", dialect, SqlUtil.compress(sql));

        FlinkDdlConverterResult result;
        try {
            // preprocess sql
            sql = preprocessSql(dialect, sql);
            // parse statements
            Statements statements = CCJSqlParserUtil.parseStatements(sql);
            // parse table comments and column comments for Oracle and DMDB
            Map<String, String> tableCommentMap = new LinkedHashMap<>();
            Map<String, String> columnCommentMap = new LinkedHashMap<>();
            parseCommentStatement(statements, targetCatalog, tableCommentMap, columnCommentMap);
            // parse create table statements for Oracle and DMDB
            result = parseCreateTableStatements(
                    dialect, statements, targetCatalog, defaultDatabase, tableCommentMap, columnCommentMap, caseStrategy);
            if (result.getCreateTableStatements().isEmpty()) {
                log.error("create table statements is empty, SQL: {}", SqlUtil.compress(sql));
                throw new IllegalDDLException("create table statements is empty");
            }
        } catch (JSQLParserException e) {
            log.error("parse SQL error:{}", SqlUtil.compress(sql));
            throw new SQLSyntaxException(e);
        }

        log.info("end parse {} SQL:{}", dialect, SqlUtil.compress(sql));
        return result;
    }

    /**
     * Parse table comments and column comments
     *
     * @see <a href="https://www.cnblogs.com/xiaoniandexigua/p/17328701.html">Comment for oracle, dmdb and mysql</a>
     * @param statements SQL statements
     * @param catalog catalog
     * @param tableCommentMap a map for table comment
     * @param columnCommentMap a map for column comment
     */
    private void parseCommentStatement(Statements statements,
                                       String catalog,
                                       Map<String, String> tableCommentMap,
                                       Map<String, String> columnCommentMap) {
        for (Statement statement : statements.getStatements()) {
            if (statement instanceof Comment) {
                Comment comment = (Comment) statement;
                // table comment
                if (comment.getTable() != null) {
                    String tableQualifier = comment.getTable().getFullyQualifiedName();
                    String tableIdentifier = String.format("`%s`.%s", catalog,
                            tableQualifier.replaceAll("\"", "`"));
                    tableCommentMap.put(tableIdentifier, comment.getComment().toString());
                }
                // column comment
                if (comment.getColumn() != null) {
                    String columnQualifier = comment.getColumn().getFullyQualifiedName();
                    columnCommentMap.put(columnQualifier, comment.getComment().toString());
                }
            }
        }
    }

    /**
     * Parse a set of CREATE-TABLE statement
     *
     * @param dialect JDBC dialect
     * @param statements statement list
     * @param targetCatalog target catalog
     * @param defaultDatabase default database
     * @param tableCommentMap table comment map
     * @param columnCommentMap column comment map
     * @param caseStrategy case strategy
     * @return FlinkSqlConverterResult
     */
    private FlinkDdlConverterResult parseCreateTableStatements(
            JdbcDialect dialect,
            Statements statements,
            String targetCatalog,
            String defaultDatabase,
            Map<String, String> tableCommentMap,
            Map<String, String> columnCommentMap,
            CaseStrategy caseStrategy) {
        List<CreateTable> createTableList = new ArrayList<>();
        for (Statement statement : statements.getStatements()) {
            if (statement instanceof CreateTable) {
                CreateTable createTable = (CreateTable) statement;
                createTableList.add(createTable);
            }
        }
        return convertCreateTableStatements(
                dialect,
                targetCatalog,
                defaultDatabase,
                createTableList,
                tableCommentMap,
                columnCommentMap,
                caseStrategy);
    }

    /**
     * Convert a set of CREATE-TABLE statement
     *
     * @param dialect  JDBC dialect
     * @param targetCatalog target catalog
     * @param defaultDatabase default default
     * @param createTableList CreateTable list
     * @param tableCommentMap table comment map
     * @param columnCommentMap column comment map
     * @param caseStrategy case strategy
     * @return FlinkSqlConverterResult
     */
    private FlinkDdlConverterResult convertCreateTableStatements(
            JdbcDialect dialect,
            String targetCatalog,
            @Nullable String defaultDatabase,
            List<CreateTable> createTableList,
            Map<String, String> tableCommentMap,
            Map<String, String> columnCommentMap,
            CaseStrategy caseStrategy) {
        // There may be same databases or tables
        Map<String, String> createDatabaseSqlMap = new LinkedHashMap<>();
        Map<String, String> createTableSqlMap = new LinkedHashMap<>();
        for (CreateTable createTable : createTableList) {
            String targetDatabase = createTable.getTable().getSchemaName();
            Preconditions.checkState(defaultDatabase != null || targetDatabase != null,
                    "database is required");
            if (targetDatabase != null) {
                targetDatabase = targetDatabase.replaceAll("\"", "");
            } else {
                targetDatabase = defaultDatabase;
            }
            String databaseIdentifier = formatDatabaseIdentifier(targetCatalog, targetDatabase, null, caseStrategy);
            String createDatabaseSql = String.format("CREATE DATABASE IF NOT EXISTS %s", databaseIdentifier);
            createDatabaseSqlMap.put(databaseIdentifier, createDatabaseSql);
            log.info("generated CREATE-DATABASE sql: {}", SqlUtil.compress(createDatabaseSql));

            String targetTable = createTable.getTable().getName().replaceAll("\"", "");
            List<String> columns = convertColumns(dialect, targetDatabase, targetTable,
                    createTable.getColumnDefinitions(), columnCommentMap, caseStrategy);
            // set table comment for Oracle and DMDB
            String tableComment = null;
            String tableIdentifier = formatTableIdentifier(
                    targetCatalog, targetDatabase, targetTable, null, null, caseStrategy);
            if (tableCommentMap != null && tableCommentMap.get(tableIdentifier) != null) {
                tableComment = tableCommentMap.get(tableIdentifier);
            }
            // generate CREATE-TABLE sql
            String createTableSql = genCreateTableSql(tableIdentifier, tableComment, columns);
            createTableSqlMap.put(tableIdentifier, createTableSql);
            log.info("generated CREATE-TABLE sql: {}", SqlUtil.compress(createTableSql));
        }
        return getFlinkSqlConverterResult(dialect, createDatabaseSqlMap, createTableSqlMap);
    }

    /**
     * Convert table column
     *
     * @param dialect JDBC dialect
     * @param targetDatabase target database
     * @param targetTable target table
     * @param columnDefinitions column definitions
     * @param columnCommentMap column comment map
     * @param caseStrategy case strategy
     * @return a set of column
     */
    private List<String> convertColumns(JdbcDialect dialect,
                                        String targetDatabase,
                                        String targetTable,
                                        List<ColumnDefinition> columnDefinitions,
                                        Map<String, String> columnCommentMap,
                                        CaseStrategy caseStrategy){
        List<String> columns = new ArrayList<>(columnDefinitions.size());
        for(ColumnDefinition col: columnDefinitions)
        {
            String columnName = col.getColumnName().replaceAll("\"", ""); // remove double quotes for oracle column
            String columnFullName = String.format("\"%s\".\"%s\".\"%s\"", targetDatabase, targetTable, columnName); // for oracle comment
            String columnTypeName = col.getColDataType().getDataType();
            List<String> columnTypeArgs = col.getColDataType().getArgumentsStringList();
            // construct field type for data source
            FieldFormat fieldFormat = convertColumnType(columnTypeName, columnTypeArgs);
            // convert to flink type
            String engineFieldType = flinkTypeConverter.toEngineType(dialect.getNodeType(), fieldFormat);
            // construct to a flink column
            StringBuilder engineColumn = new StringBuilder();
            engineColumn.append(formatColumn(columnName, caseStrategy)).append(" ").append(engineFieldType);
            if (col.getColumnSpecs() != null) {
                String columnSpec = String.join(" ", col.getColumnSpecs());
                // remove unnecessary keywords
                columnSpec = columnSpec.replaceAll("\\s?DEFAULT\\s(\\S)+\\s?", ""); // remove `DEFAULT` keyword
                columnSpec = columnSpec.replaceAll("\\s?AUTO_INCREMENT\\s?", ""); // remove `AUTO_INCREMENT` keyword for MySQL
                columnSpec = columnSpec.replaceAll("\\s?COLLATE\\s(\\S)+\\s?", ""); // remove `COLLATE utf8mb4_unicode_ci` for MySQL
                engineColumn.append(" ").append(columnSpec);
            }
            if (columnCommentMap != null && columnCommentMap.get(columnFullName) != null) {
                engineColumn.append(" COMMENT ").append(columnCommentMap.get(columnFullName));
            }
            columns.add(engineColumn.toString());
        }
        return columns;
    }

    /**
     * Convert field type for data source
     *
     * @return field type
     */
    private FieldFormat convertColumnType(String columnTypeName, List<String> columnTypeArgs) {
        Integer precision = null;
        Integer scale = null;
        if (columnTypeArgs != null) {
            if (columnTypeArgs.size() == 1){
                String arg0 = columnTypeArgs.get(0);
                if (StringUtils.isNumeric(arg0)) {
                    precision = Integer.valueOf(columnTypeArgs.get(0));
                } else if (Pattern.matches("\\d+\\s.+", arg0)) { // Oracle: VARCHAR(32 CHAR)
                    Matcher matcher = Pattern.compile("(\\d+)\\s.+").matcher(arg0);
                    if (matcher.find()) {
                        String number = matcher.group(1);
                        precision = Integer.valueOf(number);
                    }
                }
            } else if (columnTypeArgs.size() == 2) {
                String arg0 = columnTypeArgs.get(0);
                String arg1 = columnTypeArgs.get(1);
                if (StringUtils.isNumeric(arg0)) {
                    precision = Integer.valueOf(arg0);
                }
                if (StringUtils.isNumeric(arg1)) {
                    scale = Integer.valueOf(arg1);
                }
            }
        }
        return new FieldFormat(columnTypeName, precision, scale);
    }

    /**
     * Generate a CREATE TABLE statement for Flink
     *
     * @param tableIdentifier table name
     * @param tableComment table comment with single quota
     * @param flinkColumns column name list
     * @return DDL SQL
     */
    private String genCreateTableSql(String tableIdentifier, @Nullable String tableComment, List<String> flinkColumns) {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        sb.append(tableIdentifier).append(" (\n");
        for (String column : flinkColumns) {
            sb.append("    ").append(column).append(",\n");
        }
        sb.deleteCharAt(sb.length() - 2).append(")");
        if (tableComment != null) {
            sb.append(" COMMENT ").append(tableComment);
        }
        return sb.toString();
    }

    // ~ preprocessor -----------------------------------------------

    /**
     * Preprocess sql for different dialects, The purpose is to remove some keywords and clauses
     *
     * @param dialect JDBC dialect
     * @param sql a sql script
     * @return a preprocessed sql
     */
    private String preprocessSql(JdbcDialect dialect, String sql) {
        sql = preprocessSqlForDefault(sql);
        switch (dialect) {
            case Oracle:
            case DMDB:
                return preprocessSqlForOracle(sql);
            default:
                return sql;
        }
    }

    /**
     * Preprocess sql for oracle and DMDB
     *
     * @param sql a oracle sql
     * @return a preprocessed sql
     */
    private String preprocessSqlForOracle(String sql) {
        // remove some keywords and clauses
        return sql.replaceAll("\\sENABLE", "")
                .replaceAll("\\enable", "")
                .replaceAll("USING INDEX ", "")
                .replaceAll("using index ", "")
                .replaceAll(",?\\s*\n?\\s*supplemental log data.*columns", "")
                .replaceAll(",?\\s*\n?\\s*SUPPLEMENTAL LOG DATA.*COLUMNS", "");
    }

    /**
     * Preprocess sql for all dialects
     *
     * @param sql a sql
     * @return a preprocessed sql
     */
    private String preprocessSqlForDefault(String sql) {
        // TODO: It is inaccurate
        //return sql.toUpperCase()
        //        .replaceAll("\\s?DEFAULT\\s(\\S)+", ""); // remove `DEFAULT` keyword
        return sql;
    }

    // ~ formatter --------------------------------------------------

    /**
     * Format database identifier
     *
     * @param catalog catalog name
     * @param database database name
     * @param databaseAffix database prefix or suffix
     * @param caseStrategy case strategy
     * @return a database identifier, like {@code `my_catalog`.`my_db`}
     */
    private String formatDatabaseIdentifier(String catalog,
                                            String database,
                                            Affix databaseAffix,
                                            CaseStrategy caseStrategy) {
        catalog = formatQualifier(catalog, null, caseStrategy);
        database = formatQualifier(database, databaseAffix, caseStrategy);
        return String.format("%s.%s", catalog, database);
    }

    /**
     * Format table identifier
     *
     * @param catalog catalog name
     * @param database database name
     * @param table table name
     * @param caseStrategy case strategy
     * @return a table identifier, like {@code `my_catalog`.`my_db`.`my_table`}
     */
    private String formatTableIdentifier(String catalog,
                                         String database,
                                         String table,
                                         Affix databaseAffix,
                                         Affix tableAffix,
                                         CaseStrategy caseStrategy) {
        catalog = formatQualifier(catalog, null, caseStrategy);
        database = formatQualifier(database, databaseAffix, caseStrategy);
        table = formatQualifier(table, tableAffix, caseStrategy);
        return String.format("%s.%s.%s", catalog, database, table);
    }

    /**
     * Format a table field
     *
     * @param column column name
     * @param caseStrategy case strategy
     * @return formatted column name, like {@code `my_column`}
     */
    private String formatColumn(String column, CaseStrategy caseStrategy) {
        return formatQualifier(column, null, caseStrategy);
    }

    /**
     * Format and standard a qualifier from sql
     *
     * @param qualifier a qualifier, It may be a database name, a table name and so on
     * @param affix prefix or suffix
     * @param caseStrategy case strategy
     * @return formatted qualifier, like {@code `my_table`}
     */
    private String formatQualifier(String qualifier, Affix affix, CaseStrategy caseStrategy) {
        if (!qualifier.contains("`")) {
            // process affix of the qualifier
            qualifier = processQualifierAffix(qualifier, affix);
            // process case of the qualifier
            qualifier = processQualifierCase(qualifier, caseStrategy);
            // format
            return String.format("`%s`", qualifier.trim());
        }
        return qualifier; // To be optimized
    }

    /**
     * Process prefix or suffix of a qualifier
     *
     * @param qualifier a qualifier
     * @param affix prefix of suffix
     * @return formatted qualifier
     */
    private String processQualifierAffix(String qualifier, Affix affix) {
        if (affix != null) {
            switch (affix.getAffixStrategy()) {
                case PREFIX:
                    String prefix = affix.getAffixContent();
                    Preconditions.checkNotNull(prefix, "prefix is null");
                    qualifier = String.format("%s%s", prefix, qualifier);
                    break;
                case SUFFIX:
                    String suffix = affix.getAffixContent();
                    Preconditions.checkNotNull(suffix, "suffix is null");
                    qualifier = String.format("%s%s", qualifier, suffix);
                    break;
            }
        }
        return qualifier;
    }

    /**
     * Process case of a qualifier
     *
     * @param qualifier a qualifier
     * @param caseStrategy uppercase or lowercase
     * @return formatted qualifier
     */
    private String processQualifierCase(String qualifier, CaseStrategy caseStrategy) {
        switch (caseStrategy) {
            case LOWERCASE:
                return StringUtils.lowerCase(qualifier);
            case UPPERCASE:
                return StringUtils.upperCase(qualifier);
            default:
                return qualifier;
        }
    }
}

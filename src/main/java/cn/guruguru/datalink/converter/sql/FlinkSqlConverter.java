package cn.guruguru.datalink.converter.sql;

import cn.guruguru.datalink.converter.SqlConverter;
import cn.guruguru.datalink.converter.enums.DDLDialect;
import cn.guruguru.datalink.converter.sql.result.FlinkSqlConverterResult;
import cn.guruguru.datalink.converter.type.FlinkTypeConverter;
import cn.guruguru.datalink.protocol.field.FieldFormat;
import cn.guruguru.datalink.protocol.node.extract.scan.OracleScanNode;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.types.logical.LogicalType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class FlinkSqlConverter implements SqlConverter<FlinkSqlConverterResult> {

    private static final FlinkTypeConverter flinkTypeConverter = new FlinkTypeConverter();

    /**
     * Convert DDL
     *
     * @see <a href="https://techieshouts.com/home/parsing-sql-create-query-using-jsql-parser/">Parsing SQL CREATE query using JSQLParser</a>
     * @param dialect data source type
     * @param ddl DDL SQL from Data Source
     */
    @Override
    public FlinkSqlConverterResult toEngineDDL(DDLDialect dialect, String catalog, @Nullable String database, String ddl)
            throws RuntimeException {
        Preconditions.checkNotNull(dialect,"dialect is required");
        Preconditions.checkNotNull(catalog,"catalog is required");
        Preconditions.checkNotNull(ddl,"ddl is required");
        if (!ddl.trim().toUpperCase().startsWith("CREATE TABLE")) {
            log.info("Only support CREATE TABLE statement, submitted statement:" + ddl);
            throw new UnsupportedOperationException("Only support CREATE TABLE statement, submitted statement:" + ddl);
        }
        log.info("start parse {} DDL:{}", dialect, ddl);
        CreateTable createTable;
        try {
            // remove some keywords and clauses for Oracle
            ddl = ddl.replaceAll("\\sENABLE", "")
                    .replaceAll("USING INDEX ", "");
            createTable = (CreateTable) CCJSqlParserUtil.parse(ddl);
        } catch (JSQLParserException e) {
            log.error("parse CREATE TABLE SQL error:{}", ddl);
            throw new RuntimeException(e);
        }
        FlinkSqlConverterResult result;
        switch (dialect.getNodeType()) {
            case OracleScanNode.TYPE:
                result = convertOracleType(catalog, database, createTable);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported data source type:" + dialect);
        }
        log.info("end parse {} DDL:{}", dialect, ddl);
        return result;
    }

    private FlinkSqlConverterResult convertOracleType(String catalog, @Nullable String database, CreateTable createTable) {
        String tableName = createTable.getTable().getName().replaceAll("\"", "");
        String ddlDatabase = createTable.getTable().getSchemaName();
        Preconditions.checkState(database != null || ddlDatabase != null,"database is required");
        if (ddlDatabase != null) {
            ddlDatabase = ddlDatabase.replaceAll("\"", "");
        } else {
            ddlDatabase = database;
        }
        String tableIdentifier = String.format("`%s`.`%s`.`%s`", catalog, ddlDatabase, tableName);
        List<String> flinkColumns = new ArrayList<>(createTable.getColumnDefinitions().size());
        for(ColumnDefinition col: createTable.getColumnDefinitions())
        {
            String columnName = col.getColumnName().replaceAll("\"", ""); // remove double quotes for oracle column
            String columnTypeName = col.getColDataType().getDataType();
            List<String> columnTypeArgs = col.getColDataType().getArgumentsStringList();
            // construct field type for data source
            FieldFormat fieldFormat = constructFieldFormat(columnTypeName, columnTypeArgs);
            // convert to flink type
            LogicalType engineFieldType = flinkTypeConverter.toEngineType(OracleScanNode.TYPE, fieldFormat);
            // construct to a flink column
            StringBuilder engineColumn = new StringBuilder();
            engineColumn.append("`").append(columnName).append("`");
            engineColumn.append(" ").append(engineFieldType);
            if (col.getColumnSpecs() != null) {
                String columnSpec = String.join(" ", col.getColumnSpecs());
                // remove unnecessary keywords for Oracle
                columnSpec = columnSpec.replaceAll("\\sDEFAULT\\s(\\S)+", ""); // remove DEFAULT keyword
                engineColumn.append(" ").append(columnSpec);
            }
            flinkColumns.add(engineColumn.toString());
        }
        // generate CREATE TABLE statement
        String flinkDDL = generateFlinkDDL(tableIdentifier, flinkColumns);
        log.info("generated flink ddl: {}", flinkDDL.replaceAll("\\n", "").replaceAll("\\s{2,}", " ").trim());
        return new FlinkSqlConverterResult(catalog, ddlDatabase, tableName, flinkDDL);
    }

    /**
     * Construct field type for data source
     *
     * @return field type
     */
    private FieldFormat constructFieldFormat(String columnTypeName, List<String> columnTypeArgs) {
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
     * @param flinkColumns column list
     * @return DDL SQL
     */
    private String generateFlinkDDL(String tableIdentifier, List<String> flinkColumns) {
        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        sb.append(tableIdentifier).append(" (\n");
        for (String column : flinkColumns) {
            sb.append("    ").append(column).append(",\n");
        }
        sb.deleteCharAt(sb.length() - 2).append(");");
        return sb.toString();
    }
}

package cn.guruguru.datalink.converter.sql;

import cn.guruguru.datalink.converter.SqlConverter;
import cn.guruguru.datalink.converter.type.FlinkTypeConverter;
import cn.guruguru.datalink.protocol.field.FieldFormat;
import cn.guruguru.datalink.protocol.node.extract.scan.OracleScanNode;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class FlinkSqlConverter implements SqlConverter {

    private static final FlinkTypeConverter flinkTypeConverter = new FlinkTypeConverter();

    /**
     * Convert DDL
     *
     * @see <a href="https://techieshouts.com/home/parsing-sql-create-query-using-jsql-parser/">Parsing SQL CREATE query using JSQLParser</a>
     * @param dialect data source type
     * @param ddl DDL SQL from Data Source
     */
    @Override
    public String toEngineDDL(String dialect, String ddl) {
        Preconditions.checkNotNull(dialect,"sourceType is required");
        Preconditions.checkNotNull(ddl,"ddl is required");
        if (!ddl.trim().toUpperCase().startsWith("CREATE TABLE")) {
            throw new UnsupportedOperationException("Only support CREATE TABLE statement:" + ddl);
        }
        log.info("start parse {} DDL:{}", dialect, ddl);
        CreateTable createTable;
        try {
            createTable = (CreateTable) CCJSqlParserUtil.parse(ddl);
        } catch (JSQLParserException e) {
            log.error("parse CREATE TABLE SQL error:{}", ddl);
            throw new RuntimeException(e);
        }
        String engineDDL;
        switch (dialect) {
            case "Oracle":
                engineDDL = convertOracleType(createTable);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported data source type:" + dialect);
        }
        log.info("end parse {} DDL:{}", dialect, ddl);
        return engineDDL;
    }

    private String convertOracleType(CreateTable createTable) {
        String tableIdentifier = createTable.getTable().getFullyQualifiedName();
        List<String> flinkColumns = new ArrayList<>(createTable.getColumnDefinitions().size());
        for(ColumnDefinition col: createTable.getColumnDefinitions())
        {
            String columnName = col.getColumnName();
            String columnTypeName = col.getColDataType().getDataType();
            List<String> columnTypeArgs = col.getColDataType().getArgumentsStringList();
            // construct field type for data source
            FieldFormat fieldFormat = constructFieldFormat(columnTypeName, columnTypeArgs);
            // convert to flink type
            LogicalType engineFieldFormat = flinkTypeConverter.toEngineType(OracleScanNode.TYPE, fieldFormat);
            // construct to a flink column
            StringBuilder engineColumn = new StringBuilder(columnName);
            engineColumn.append(" ").append(engineFieldFormat);
            if (col.getColumnSpecs() != null) {
                String columnSpec = String.join(" ", col.getColumnSpecs());
                engineColumn.append(" ").append(columnSpec);
            }
            flinkColumns.add(engineColumn.toString());
        }
        // generate `CREATE TABLE` statement
        String flinkDDL = generateFlinkDDL(tableIdentifier, flinkColumns);
        log.info("generated spark ddl: {}", flinkDDL);
        return flinkDDL;
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
                precision = Integer.valueOf(columnTypeArgs.get(0));
            } else if (columnTypeArgs.size() == 2){
                precision = Integer.valueOf(columnTypeArgs.get(0));
                scale = Integer.valueOf(columnTypeArgs.get(1));
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
        sb.deleteCharAt(sb.length() - 2).append(")");
        return sb.toString();
    }
}

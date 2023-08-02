package cn.guruguru.datalink.types;

import cn.guruguru.datalink.formats.FieldFormat;
import cn.guruguru.datalink.protocol.node.ExtractNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

public class FlinkSqlTypeMapper implements TypeMapper {

    @Override
    public String deriveEngineSql(ExtractNode extractNode, String ddl) {
        String sourceType = ExtractNode.class.getAnnotation(JsonTypeName.class).value();
        return null;
    }

    /**
     * Derive the engine type for the given datasource field type
     *
     * @see org.apache.Flink.sort.formats.base.TableFormatUtils#deriveLogicalType(FormatInfo) 
     * @param extractNode extract node
     * @param fieldFormat source field
     */
    @Override
    public FieldFormat deriveEngineType(ExtractNode extractNode, FieldFormat fieldFormat) {
        String sourceType = extractNode.getClass().getAnnotation(JsonTypeName.class).value();
        String fieldName = fieldFormat.getField();
        String fieldType = StringUtils.upperCase(fieldFormat.getType());
        Integer precision = fieldFormat.getPrecision();
        Integer scale = fieldFormat.getScale();
        String engineType;
        switch (sourceType) {
            case "oracle-scan":
                engineType = convertOracleTypeToInFlinkType(fieldType, precision, scale);
                break;
            case "mysql-scan":
                engineType = convertMysqlTypeToFlinkType(fieldType, precision, scale);
                break;
            case "oracle-cdc":
                engineType = convertOracleCdcTypeToInFlinkType(fieldType, precision, scale);
                break;
            case "lakehouse-load":
                engineType = convertArcticMixedIcebergTypeToInFlinkType(fieldType, precision, scale);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported data source type");
        }
        return new FieldFormat(fieldName, engineType, precision, scale);
    }

    // ~ type converter --------------------------------------------------

    /**
     * Convert Mysql type to Flink type
     *
     * @see <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/jdbc/#data-type-mapping">Data Type Mapping</a>
     * @param fieldType Mysql Field Type
     * @param precision field precision
     * @param scale field scale
     * @return Flink SQL Field Type
     */
    private String convertMysqlTypeToFlinkType(String fieldType, Integer precision, Integer scale) {
        switch (fieldType) {
            case "TINYINT":
                return "TINYINT";
            case "SMALLINT":
            case "TINYINT UNSIGNED":
                return "SMALLINT";
            case "INT":
            case "MEDIUMINT":
            case "SMALLINT UNSIGNED":
                return "INT";
            case "BIGINT":
            case "INT UNSIGNED": // ?
                return "BIGINT";
            case "BIGINT UNSIGNED": // ?
                return "DECIMAL(20, 0)";
            case "FLOAT":
                return "FLOAT";
            case "DOUBLE":
            case "DOUBLE PRECISION":
                return "DOUBLE";
            case "NUMERIC": // NUMERIC(p, s)
            case "DECIMAL": // DECIMAL(p, s)
                return "DECIMAL(p, s)";
            case "BOOLEAN":
            case "TINYINT(1)":
                return "BOOLEAN";
            case "DATE":
                return "DATE";
            case "TIME [(p)]":
                return "TIME [(p)] [WITHOUT TIMEZONE]";
            case "DATETIME [(p)]":
                return "TIMESTAMP [(p)] [WITHOUT TIMEZONE]";
            case "CHAR": // CHAR(n)
            case "VARCHAR": // VARCHAR(n)
            case "TEXT":
                return "STRING";
            case "BINARY":
            case "VARBINARY":
            case "BLOB":
                return "BYTES";
            default:
                throw new UnsupportedOperationException("Unsupported data type");
        }
    }

    /**
     *
     * Convert Oracle type to Flink type
     *
     * @see <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/jdbc/#data-type-mapping">Data Type Mapping</a>
     * @param fieldType Oracle Field Type
     * @return Flink SQL Field Type
     */
    private String convertOracleTypeToInFlinkType(String fieldType, Integer precision, Integer scale) {
        switch (fieldType) {
            case "BINARY_FLOAT":
                return "FLOAT";
            case "BINARY_DOUBLE":
                return "DOUBLE";
            case "SMALLINT":
            case "FLOAT": // FLOAT(s)
            case "DOUBLE PRECISION":
            case "REAL":
            case "NUMBER": // NUMBER(p, s)
                return "DECIMAL"; // NUMBER(p, s)
            case "DATE":
                return "DATE";
            case "TIMESTAMP": // TIMESTAMP [(p)] [WITHOUT TIMEZONE]
                return "TIMESTAMP"; // TIMESTAMP [(p)] [WITHOUT TIMEZONE]
            case "CHAR": // CHAR(n)
            case "VARCHAR": // VARCHAR(n)
            case "CLOB":
                return "STRING";
            case "RAW": // RAW(s)
            case "BLOB":
                return "BYTES";
            default:
                throw new UnsupportedOperationException("Unsupported data type");
        }
    }

    /**
     * Convert Oracle CDC type to Flink type
     *
     * @see <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/jdbc/#data-type-mapping">Data Type Mapping</a>
     * @param fieldType Oracle CDC Field Type
     * @return Flink SQL Field Type
     */
    private String convertOracleCdcTypeToInFlinkType(String fieldType, Integer precision, Integer scale) {
        switch (fieldType) {
            case "NUMBER":
                if (scale <= 0 && precision - scale < 3) { // NUMBER(p, s <= 0), p - s < 3
                    return "TINYINT";
                } else if (scale <= 0 && precision - scale < 5) { // NUMBER(p, s <= 0), p - s < 5
                    return "SMALLINT";
                } else if (scale <= 0 && precision - scale < 10) { // NUMBER(p, s <= 0), p - s < 10
                    return "INT";
                } else if (scale <= 0 && precision - scale < 19) { // NUMBER(p, s <= 0), p - s < 19
                    return "BIGINT";
                } else if (scale > 0 && precision - scale >= 19 && precision - scale <= 38) { // NUMBER(p, s <= 0), 19 <= p - s <= 38
                    return "DECIMAL(p - s, 0)";
                } else if (scale <= 0 && precision - scale > 38) { // NUMBER(p, s <= 0), p - s > 38
                    return "STRING";
                } else if (scale > 0) { // NUMBER(p, s > 0)
                    return  "DECIMAL"; // DECIMAL(p, s)
                }
            case "FLOAT":
            case "BINARY_FLOAT":
                return "FLOAT";
            case "BINARY_DOUBLE": // DOUBLE PRECISION
                return "DOUBLE";
            case "NUMBER(1)":
                return "BOOLEAN";
            case "DATE":
            case "TIMESTAMP [(p)]":
                return "TIMESTAMP [(p)] [WITHOUT TIMEZONE]";
            case "TIMESTAMP [(p)] WITH TIME ZONE":
                return "TIMESTAMP [(p)] WITH TIME ZONE";
            case "TIMESTAMP [(p)] WITH LOCAL TIME ZONE":
                return "TIMESTAMP_LTZ [(p)]";
            case "CHAR(n)":
            case "NCHAR(n)":
            case "NVARCHAR2(n)":
            case "VARCHAR(n)":
            case "VARCHAR2(n)":
            case "CLOB":
            case "NCLOB":
            case "XMLType":
            case "SYS.XMLTYPE":
                return "STRING";
            case "BLOB":
            case "ROWID":
                return "BYTES";
            case "INTERVAL DAY TO SECOND":
            case "INTERVAL YEAR TO MONTH":
                return "BIGINT";
            default:
                throw new UnsupportedOperationException("Unsupported data type");
        }
    }

  /**
   * Convert Arctic mixed iceberg type to Flink type
   *
   * @see <a href="https://arctic.netease.com/ch/flink/flink-ddl/#mixed-iceberg-data-types">Mixed Iceberg Data Types</a>
   * @param fieldType Arctic Mixed Iceberg Field Type
   * @return Flink SQL Field Type
   */
  private String convertArcticMixedIcebergTypeToInFlinkType(
      String fieldType, Integer precision, Integer scale) {
        switch (fieldType) {
            case "STRING":
                return "STRING";
            case "BOOLEAN":
                return "BOOLEAN";
            case "INT":
                return "INT";
            case "LONG":
                return "BIGINT";
            case "FLOAT":
                return "FLOAT";
            case "DOUBLE":
                return "DOUBLE";
            case "DECIMAL": // DECIMAL(p, s)
                return "DECIMAL"; // DECIMAL(p, s)
            case "DATE":
                return "DATE";
            case "TIMESTAMP":
                return "TIMESTAMP"; // TIMESTAMP(6)
            case "TIMESTAMPTZ":
                return "TIMESTAMP WITH LOCAL TIME ZONE"; // TIMESTAMP(6) WITH LOCAL TIME ZONE
            case "FIXED": // FIXED(p)
                return "BINARY"; // BINARY(p)
            case "UUID":
                return "BINARY"; // BINARY(16)
            case "BINARY":
                return "VARBINARY";
            case "ARRAY":
                return "ARRAY";
            case "MAP":
                return "MAP"; // or: "MULTISET"
            case "STRUCT":
                return "ROW";
            default:
                throw new UnsupportedOperationException("Unsupported data type");
        }
    }
}

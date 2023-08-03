package cn.guruguru.datalink.converter.type;

import cn.guruguru.datalink.converter.TypeConverter;
import cn.guruguru.datalink.protocol.field.FieldFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

public class FlinkTypeConverter implements TypeConverter {

    /**
     * Derive the engine type for the given datasource field type
     *
     * @see org.apache.Flink.sort.formats.base.TableFormatUtils#deriveLogicalType(FormatInfo) 
     * @param nodeType node type
     * @param fieldFormat source field
     */
    @Override
    public FieldFormat toEngineType(String nodeType, FieldFormat fieldFormat) {
        switch (nodeType) {
            case "MysqlScan":
                return convertMysqlType(fieldFormat);
            case "OracleScan":
                return convertOracleType(fieldFormat);
            case "OracleCdc":
                return convertOracleCdcType(fieldFormat);
            case "LakehouseLoad":
                return convertArcticMixedIcebergType(fieldFormat);
            default:
                throw new UnsupportedOperationException("Unsupported data source type:" + nodeType);
        }
    }

    // ~ type converter --------------------------------------------------

    /**
     * Convert Mysql type to Flink type
     *
     * @see <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/jdbc/#data-type-mapping">Data Type Mapping</a>
     * @param fieldFormat mysql field format
     * @return Flink SQL Field Type
     */
    private FieldFormat convertMysqlType(FieldFormat fieldFormat) {
        String fieldType = StringUtils.upperCase(fieldFormat.getType());
        Integer precision = fieldFormat.getPrecision();
        Integer scale = fieldFormat.getScale();
        switch (fieldType) {
            case "TINYINT":
                if (precision == 1) { // TINYINT(1)
                    return new FieldFormat(new BooleanType().asSummaryString(), null, null);
                } else {
                    return new FieldFormat(new TinyIntType().asSummaryString(), null, null);
                }
            case "SMALLINT":
            case "TINYINT UNSIGNED":
                return new FieldFormat(new SmallIntType().asSummaryString(), null, null);
            case "INT":
            case "MEDIUMINT":
            case "SMALLINT UNSIGNED":
                return new FieldFormat(new IntType().asSummaryString(), null, null);
            case "BIGINT":
            case "INT UNSIGNED": // TODO
                return new FieldFormat(new BigIntType().asSummaryString(), null, null);
            case "BIGINT UNSIGNED": // TODO
                return new FieldFormat(new DecimalType().asSummaryString(), 20, 0);
            case "FLOAT":
                return new FieldFormat(new FloatType().asSummaryString(), null, null);
            case "DOUBLE":
            case "DOUBLE PRECISION":
                return new FieldFormat(new DoubleType().asSummaryString(), 20, 0);
            case "NUMERIC": // NUMERIC(p, s)
            case "DECIMAL": // DECIMAL(p, s)
                return new FieldFormat(new DecimalType().asSummaryString(), precision, scale);
            case "BOOLEAN": // TINYINT(1)
                return new FieldFormat(new BooleanType().asSummaryString(), null, null);
            case "DATE":
                return new FieldFormat(new DateType().asSummaryString(), null, null);
            case "TIME": // TIME [(p)]
                return new FieldFormat(new TimeType().asSummaryString(), precision, null); // TIME [(p)] [WITHOUT TIMEZONE]
            case "DATETIME": // DATETIME [(p)]
                return new FieldFormat(new TimestampType().asSummaryString(), precision, null); // TIMESTAMP [(p)] [WITHOUT TIMEZONE]
            case "CHAR": // CHAR(n)
            case "VARCHAR": // VARCHAR(n)
            case "TEXT":
                return new FieldFormat(new VarCharType(VarCharType.MAX_LENGTH).asSummaryString(), null, null); // STRING
            case "BINARY":
            case "VARBINARY":
            case "BLOB":
                return new FieldFormat(new VarBinaryType(VarBinaryType.MAX_LENGTH).asSummaryString(), null, null); // BYTES
            default:
                throw new UnsupportedOperationException("Unsupported MySQL data type:" + fieldType);
        }
    }

    /**
     *
     * Convert Oracle type to Flink type
     *
     * @see <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/jdbc/#data-type-mapping">Data Type Mapping</a>
     * @param fieldFormat Oracle Field Type
     * @return Flink SQL Field Type
     */
    private FieldFormat convertOracleType(FieldFormat fieldFormat) {
        String fieldType = StringUtils.upperCase(fieldFormat.getType());
        Integer precision = fieldFormat.getPrecision();
        Integer scale = fieldFormat.getScale();
        switch (fieldType) {
            case "BINARY_FLOAT":
                return new FieldFormat(new FloatType().asSummaryString(), null, null);
            case "BINARY_DOUBLE":
                return new FieldFormat(new DoubleType().asSummaryString(), null, null);
            case "SMALLINT":
            case "FLOAT": // FLOAT(s)
            case "DOUBLE PRECISION":
            case "REAL":
            case "NUMBER": // NUMBER(p, s)
                return new FieldFormat(new DecimalType().asSummaryString(), precision, scale);
            case "DATE":
                return new FieldFormat(new DateType().asSummaryString(), null, null);
            case "TIMESTAMP": // TODO: TIMESTAMP [(p)] [WITHOUT TIMEZONE]
                return new FieldFormat(new TimestampType().asSummaryString(), precision, null); // TIMESTAMP [(p)] [WITHOUT TIMEZONE]
            case "CHAR": // CHAR(n)
            case "VARCHAR": // VARCHAR(n)
            case "CLOB":
                return new FieldFormat(new VarCharType(VarCharType.MAX_LENGTH).asSummaryString(), null, null);
            case "RAW": // RAW(s)
            case "BLOB":
                return new FieldFormat(new VarBinaryType(VarBinaryType.MAX_LENGTH).asSummaryString(), null, null);
            default:
                throw new UnsupportedOperationException("Unsupported Oracle data type:" + fieldType);
        }
    }

    /**
     * Convert Oracle CDC type to Flink type
     *
     * @see <a href="https://ververica.github.io/flink-cdc-connectors/master/content/connectors/oracle-cdc.html#data-type-mapping">Data Type Mapping</a>
     * @param fieldFormat Oracle CDC Field Type
     * @return Flink SQL Field Type
     */
    private FieldFormat convertOracleCdcType(FieldFormat fieldFormat) {
        String fieldType = StringUtils.upperCase(fieldFormat.getType());
        Integer precision = fieldFormat.getPrecision();
        Integer scale = fieldFormat.getScale();
        switch (fieldType) {
            case "NUMBER":
                if (precision == 1) { // NUMBER(1)
                    return new FieldFormat(new BooleanType().asSummaryString(), null, null);
                } else if (scale <= 0 && precision - scale < 3) { // NUMBER(p, s <= 0), p - s < 3
                    return new FieldFormat(new TinyIntType().asSummaryString(), null, null);
                } else if (scale <= 0 && precision - scale < 5) { // NUMBER(p, s <= 0), p - s < 5
                    return new FieldFormat(new SmallIntType().asSummaryString(), null, null);
                } else if (scale <= 0 && precision - scale < 10) { // NUMBER(p, s <= 0), p - s < 10
                    return new FieldFormat(new IntType().asSummaryString(), null, null);
                } else if (scale <= 0 && precision - scale < 19) { // NUMBER(p, s <= 0), p - s < 19
                    return new FieldFormat(new BigIntType().asSummaryString(), null, null);
                } else if (scale > 0 && precision - scale >= 19 && precision - scale <= 38) { // NUMBER(p, s <= 0), 19 <= p - s <= 38
                    return new FieldFormat(new DecimalType().asSummaryString(), precision - scale, 0);
                } else if (scale <= 0 && precision - scale > 38) { // NUMBER(p, s <= 0), p - s > 38
                    return new FieldFormat(new VarCharType(VarCharType.MAX_LENGTH).asSummaryString(), null, null);
                } else if (scale > 0) { // NUMBER(p, s > 0)
                    return new FieldFormat(new DecimalType().asSummaryString(), precision, scale);
                }
            case "FLOAT":
            case "BINARY_FLOAT":
                return new FieldFormat(new FloatType().asSummaryString(), null, null);
            case "DOUBLE PRECISION":
            case "BINARY_DOUBLE":
                return new FieldFormat(new DecimalType().asSummaryString(), null, null);
            case "DATE":
            case "TIMESTAMP": // TIMESTAMP [(p)]
                return new FieldFormat(new TimestampType().asSummaryString(), precision, null); // TIMESTAMP [(p)] [WITHOUT TIMEZONE]
            case "TIMESTAMP [(p)] WITH TIME ZONE": // TODO
                return new FieldFormat(new ZonedTimestampType().asSummaryString(), precision, null); // TIMESTAMP [(p)] WITH TIME ZONE
            case "TIMESTAMP [(p)] WITH LOCAL TIME ZONE": // TODO
                return new FieldFormat(new LocalZonedTimestampType().asSummaryString(), precision, null); // TIMESTAMP_LTZ [(p)]
            case "CHAR": // CHAR(n)
            case "NCHAR": // NCHAR(n)
            case "NVARCHAR2": // NVARCHAR2(n)
            case "VARCHAR": // VARCHAR(n)
            case "VARCHAR2": // VARCHAR2(n)
            case "CLOB":
            case "NCLOB":
            case "XMLType":
            case "SYS.XMLTYPE":
                return new FieldFormat(new VarCharType(VarCharType.MAX_LENGTH).asSummaryString(), null, null);
            case "BLOB":
            case "ROWID":
                return new FieldFormat(new VarBinaryType(VarBinaryType.MAX_LENGTH).asSummaryString(), null, null);
            case "INTERVAL DAY TO SECOND":
            case "INTERVAL YEAR TO MONTH":
                return new FieldFormat(new BigIntType().asSummaryString(), null, null);
            default:
                throw new UnsupportedOperationException("Unsupported Oracle CDC data type:" + fieldType);
        }
    }

  /**
   * Convert Arctic mixed iceberg type to Flink type
   *
   * @see <a href="https://arctic.netease.com/ch/flink/flink-ddl/#mixed-iceberg-data-types">Mixed Iceberg Data Types</a>
   * @param fieldFormat Arctic Mixed Iceberg Field Type
   * @return Flink SQL Field Type
   */
  private FieldFormat convertArcticMixedIcebergType(FieldFormat fieldFormat) {
      String fieldType = StringUtils.upperCase(fieldFormat.getType());
      Integer precision = fieldFormat.getPrecision();
      Integer scale = fieldFormat.getScale();
      switch (fieldType) {
          case "STRING":
              return new FieldFormat(new VarCharType(VarCharType.MAX_LENGTH).asSummaryString(), null, null);
          case "BOOLEAN":
              return new FieldFormat(new BooleanType().asSummaryString(), null, null);
          case "INT":
              return new FieldFormat(new IntType().asSummaryString(), null, null);
          case "LONG":
              return new FieldFormat(new BigIntType().asSummaryString(), null, null);
          case "FLOAT":
              return new FieldFormat(new FloatType().asSummaryString(), null, null);
          case "DOUBLE":
              return new FieldFormat(new DoubleType().asSummaryString(), null, null);
          case "DECIMAL": // DECIMAL(p, s)
              return new FieldFormat(new DecimalType().asSummaryString(), precision, scale);
          case "DATE":
              return new FieldFormat(new DateType().asSummaryString(), null, null);
          case "TIMESTAMP":
              return new FieldFormat(new TimestampType().asSummaryString(), TimestampType.DEFAULT_PRECISION, null); // TIMESTAMP(6)
          case "TIMESTAMPTZ":
              return new FieldFormat(new LocalZonedTimestampType().asSerializableString(), TimestampType.DEFAULT_PRECISION, null); // TIMESTAMP(6) WITH LCOAL TIME ZONE
          case "FIXED": // FIXED(p)
              return new FieldFormat(new VarBinaryType(VarBinaryType.MAX_LENGTH).asSummaryString(), precision, null); // BINARY(p)
          case "UUID":
              return new FieldFormat(new VarBinaryType(VarBinaryType.MAX_LENGTH).asSummaryString(), 16, null); // BINARY(16)
          case "BINARY":
              return new FieldFormat(new VarBinaryType(VarBinaryType.MAX_LENGTH).asSummaryString(), null, null);
          case "ARRAY": // TODO
              return new FieldFormat("ARRAY", null, null);
          case "MAP": // TODO
              return new FieldFormat("MAP", null, null); // or: MULTISET
          case "STRUCT": // TODO
              return new FieldFormat("ROW", null, null);
          default:
              throw new UnsupportedOperationException("Unsupported Lakehouse data type:" + fieldType);
        }
    }
}

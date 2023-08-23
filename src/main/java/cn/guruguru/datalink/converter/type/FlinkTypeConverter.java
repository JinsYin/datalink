package cn.guruguru.datalink.converter.type;

import cn.guruguru.datalink.converter.TypeConverter;
import cn.guruguru.datalink.exception.UnsupportedDataSourceException;
import cn.guruguru.datalink.exception.UnsupportedDataTypeException;
import cn.guruguru.datalink.protocol.field.FieldFormat;
import cn.guruguru.datalink.protocol.node.extract.cdc.OracleCdcNode;
import cn.guruguru.datalink.protocol.node.extract.scan.MySqlScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.OracleScanNode;
import cn.guruguru.datalink.protocol.node.load.LakehouseLoadNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import java.util.Collections;

public class FlinkTypeConverter implements TypeConverter<LogicalType> {

    /**
     * Derive the engine type for the given datasource field type
     *
     * @see org.apache.Flink.sort.formats.base.TableFormatUtils#deriveLogicalType(FormatInfo) 
     * @param nodeType node type
     * @param fieldFormat source field
     */
    @Override
    public LogicalType toEngineType(String nodeType, FieldFormat fieldFormat) {
        switch (nodeType) {
            case MySqlScanNode.TYPE:
                return convertMysqlType(fieldFormat);
            case OracleScanNode.TYPE:
                return convertOracleType(fieldFormat);
            case OracleCdcNode.TYPE:
                return convertOracleCdcType(fieldFormat);
            case LakehouseLoadNode.TYPE:
                return convertArcticMixedIcebergType(fieldFormat);
            default:
                throw new UnsupportedDataSourceException("Unsupported data source type:" + nodeType);
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
    private LogicalType convertMysqlType(FieldFormat fieldFormat) {
        String fieldType = StringUtils.upperCase(fieldFormat.getType());
        Integer precision = fieldFormat.getPrecision();
        Integer scale = fieldFormat.getScale();
        switch (fieldType) {
            case "TINYINT":
                if (precision == 1) { // TINYINT(1)
                    return new BooleanType();
                } else {
                    return new TinyIntType();
                }
            case "SMALLINT":
            case "TINYINT UNSIGNED":
                return new SmallIntType();
            case "INT":
            case "MEDIUMINT":
            case "SMALLINT UNSIGNED":
                return new IntType();
            case "BIGINT":
            case "INT UNSIGNED": // TODO
                return new BigIntType();
            case "BIGINT UNSIGNED": // TODO
                return new DecimalType(20, 0);
            case "FLOAT":
                return new FloatType();
            case "DOUBLE":
            case "DOUBLE PRECISION":
                return new DoubleType();
            case "NUMERIC": // NUMERIC(p, s)
            case "DECIMAL": // DECIMAL(p, s)
                return new DecimalType(precision, scale);
            case "BOOLEAN": // TINYINT(1)
                return new BooleanType();
            case "DATE":
                return new DateType();
            case "TIME": // TIME [(p)]
                return new TimeType(precision); // TIME [(p)] [WITHOUT TIMEZONE]
            case "DATETIME": // DATETIME [(p)]
                return new TimestampType(precision); // TIMESTAMP [(p)] [WITHOUT TIMEZONE]
            case "CHAR": // CHAR(n)
            case "VARCHAR": // VARCHAR(n)
            case "TEXT":
                return new VarCharType(VarCharType.MAX_LENGTH); // STRING
            case "BINARY":
            case "VARBINARY":
            case "BLOB":
                return new VarBinaryType(VarBinaryType.MAX_LENGTH); // BYTES
            default:
                throw new UnsupportedDataTypeException("Unsupported MySQL data type:" + fieldType);
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
    private LogicalType convertOracleType(FieldFormat fieldFormat) {
        String fieldType = StringUtils.upperCase(fieldFormat.getType());
        Integer precision = fieldFormat.getPrecision();
        Integer scale = fieldFormat.getScale();
        switch (fieldType) {
            case "BINARY_FLOAT":
                return new FloatType();
            case "BINARY_DOUBLE":
                return new DoubleType();
            case "SMALLINT":
            case "FLOAT": // FLOAT(s)
            case "DOUBLE PRECISION":
            case "REAL":
            case "NUMBER": // NUMBER(p, s)
                if (precision != null && scale != null) {
                    return new DecimalType(precision, scale);
                } else if (precision != null) {
                    return new DecimalType(precision);
                } else {
                    return new DecimalType();
                }
            case "DATE":
                return new DateType();
            case "TIMESTAMP": // TODO: TIMESTAMP [(p)] [WITHOUT TIMEZONE]
                return new TimestampType(precision); // TIMESTAMP [(p)] [WITHOUT TIMEZONE]
            case "CHAR": // CHAR(n)
            case "VARCHAR": // VARCHAR(n)
            case "VARCHAR2": // it is not mentioned in the Flink document
            case "CLOB":
                return new VarCharType(VarCharType.MAX_LENGTH);
            case "RAW": // RAW(s)
            case "BLOB":
                return new VarBinaryType(VarBinaryType.MAX_LENGTH);
            default:
                throw new UnsupportedDataTypeException("Unsupported Oracle data type:" + fieldType);
        }
    }

    /**
     * Convert Oracle CDC type to Flink type
     *
     * @see <a href="https://ververica.github.io/flink-cdc-connectors/master/content/connectors/oracle-cdc.html#data-type-mapping">Data Type Mapping</a>
     * @param fieldFormat Oracle CDC Field Type
     * @return Flink SQL Field Type
     */
    private LogicalType convertOracleCdcType(FieldFormat fieldFormat) {
        String fieldType = StringUtils.upperCase(fieldFormat.getType());
        Integer precision = fieldFormat.getPrecision();
        Integer scale = fieldFormat.getScale();
        switch (fieldType) {
            case "NUMBER":
                if (precision == 1) { // NUMBER(1)
                    return new BooleanType();
                } else if (scale <= 0 && precision - scale < 3) { // NUMBER(p, s <= 0), p - s < 3
                    return new TinyIntType();
                } else if (scale <= 0 && precision - scale < 5) { // NUMBER(p, s <= 0), p - s < 5
                    return new SmallIntType();
                } else if (scale <= 0 && precision - scale < 10) { // NUMBER(p, s <= 0), p - s < 10
                    return new IntType();
                } else if (scale <= 0 && precision - scale < 19) { // NUMBER(p, s <= 0), p - s < 19
                    return new BigIntType();
                } else if (scale > 0 && precision - scale >= 19 && precision - scale <= 38) { // NUMBER(p, s <= 0), 19 <= p - s <= 38
                    return new DecimalType(precision - scale, 0);
                } else if (scale <= 0 && precision - scale > 38) { // NUMBER(p, s <= 0), p - s > 38
                    return new VarCharType(VarCharType.MAX_LENGTH);
                } else if (scale > 0) { // NUMBER(p, s > 0)
                    return new DecimalType(precision, scale);
                }
            case "FLOAT":
            case "BINARY_FLOAT":
                return new FloatType();
            case "DOUBLE PRECISION":
            case "BINARY_DOUBLE":
                return new DecimalType();
            case "DATE":
            case "TIMESTAMP": // TIMESTAMP [(p)]
                return new TimestampType(precision); // TIMESTAMP [(p)] [WITHOUT TIMEZONE]
            case "TIMESTAMP [(p)] WITH TIME ZONE": // TODO
                return new ZonedTimestampType(precision); // TIMESTAMP [(p)] WITH TIME ZONE
            case "TIMESTAMP [(p)] WITH LOCAL TIME ZONE": // TODO
                return new LocalZonedTimestampType(precision); // TIMESTAMP_LTZ [(p)]
            case "CHAR": // CHAR(n)
            case "NCHAR": // NCHAR(n)
            case "NVARCHAR2": // NVARCHAR2(n)
            case "VARCHAR": // VARCHAR(n)
            case "VARCHAR2": // VARCHAR2(n)
            case "CLOB":
            case "NCLOB":
            case "XMLType":
            case "SYS.XMLTYPE":
                return new VarCharType(VarCharType.MAX_LENGTH);
            case "BLOB":
            case "ROWID":
                return new VarBinaryType(VarBinaryType.MAX_LENGTH);
            case "INTERVAL DAY TO SECOND":
            case "INTERVAL YEAR TO MONTH":
                return new BigIntType();
            default:
                throw new UnsupportedDataTypeException("Unsupported Oracle CDC data type:" + fieldType);
        }
    }

  /**
   * Convert Arctic mixed iceberg type to Flink type
   *
   * @see <a href="https://arctic.netease.com/ch/flink/flink-ddl/#mixed-iceberg-data-types">Mixed Iceberg Data Types</a>
   * @see <a href="https://github.com/DTStack/chunjun/blob/master/chunjun-connectors/chunjun-connector-arctic/src/main/java/com/dtstack/chunjun/connector/arctic/converter/ArcticRawTypeMapper.java">ArcticRawTypeMapper</a>
   * @param fieldFormat Arctic Mixed Iceberg Field Type
   * @return Flink SQL Field Type
   */
  private LogicalType convertArcticMixedIcebergType(FieldFormat fieldFormat) {
      String fieldType = StringUtils.upperCase(fieldFormat.getType());
      Integer precision = fieldFormat.getPrecision();
      Integer scale = fieldFormat.getScale();
      switch (fieldType) {
          case "STRING":
              return VarCharType.STRING_TYPE;
          case "BOOLEAN":
              return new BooleanType();
          case "INT":
              return new IntType();
          case "LONG":
              return new BigIntType();
          case "FLOAT":
              return new FloatType();
          case "DOUBLE":
              return new DoubleType();
          case "DECIMAL": // DECIMAL(p, s)
              return new DecimalType(precision, scale);
          case "DATE":
              return new DateType();
          case "TIMESTAMP":
              return new TimestampType(TimestampType.DEFAULT_PRECISION); // TIMESTAMP(6)
          case "TIMESTAMPTZ":
              return new LocalZonedTimestampType(TimestampType.DEFAULT_PRECISION); // TIMESTAMP(6) WITH LCOAL TIME ZONE
          case "FIXED": // FIXED(p)
              return new VarBinaryType(precision); // BINARY(p)
          case "UUID":
              return new VarBinaryType(16); // BINARY(16)
          case "BINARY": // TODO ?
              return new VarBinaryType(precision);
          case "ARRAY": // TODO
              return new ArrayType(VarCharType.STRING_TYPE);
          case "MAP": // TODO
              return new MapType(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE); // or: MULTISET
          case "STRUCT": // TODO
              return new RowType(Collections.emptyList());
          default:
              throw new UnsupportedDataTypeException("Unsupported Lakehouse data type:" + fieldType);
        }
    }
}

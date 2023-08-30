package cn.guruguru.datalink.converter.type;

import cn.guruguru.datalink.converter.TypeConverter;
import cn.guruguru.datalink.exception.UnsupportedDataSourceException;
import cn.guruguru.datalink.exception.UnsupportedDataTypeException;
import cn.guruguru.datalink.protocol.field.FieldFormat;
import cn.guruguru.datalink.protocol.node.extract.cdc.OracleCdcNode;
import cn.guruguru.datalink.protocol.node.extract.scan.DmScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.MySqlScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.OracleScanNode;
import cn.guruguru.datalink.protocol.node.load.LakehouseLoadNode;
import lombok.extern.slf4j.Slf4j;
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

@Slf4j
public class FlinkTypeConverter implements TypeConverter<String> {

    /**
     * Derive the engine type for the given datasource field type
     *
     * <pre>org.apache.inlong.sort.formats.base.TableFormatUtils#deriveLogicalType(FormatInfo)</pre>
     * @param nodeType node type
     * @param fieldFormat source field
     */
    @Override
    public String toEngineType(String nodeType, FieldFormat fieldFormat) {
        switch (nodeType) {
            case MySqlScanNode.TYPE:
                return convertMysqlType(fieldFormat).asSummaryString();
            case OracleScanNode.TYPE:
                return convertOracleType(fieldFormat).asSummaryString();
            case DmScanNode.TYPE:
                return convertDmdbForOracleType(fieldFormat).asSummaryString();
            case OracleCdcNode.TYPE:
                return convertOracleCdcType(fieldFormat).asSummaryString();
            case LakehouseLoadNode.TYPE:
                // lakehouse table may be created by Spark, so it is necessary to convert the lakehouse type to the Flink type
                // e.g. Spark `TIMESTAMP` -> Arctic `TIMESTAMPTZ` -> Flink `TIMESTAMP(6) WITH LOCAL TIME ZONE`
                return fieldFormat.getType(); // return convertLakehouseMixedIcebergType(fieldFormat).asSummaryString();
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
                return formatDecimalType(precision, scale);
            case "BOOLEAN": // TINYINT(1)
                return new BooleanType();
            case "DATE":
                return new DateType();
            case "TIME": // TIME [(p)]
                return formatTimeType(precision); // TIME [(p)] [WITHOUT TIMEZONE]
            case "DATETIME": // DATETIME [(p)]
                return formatTimestampType(precision); // TIMESTAMP [(p)] [WITHOUT TIMEZONE]
            case "CHAR": // CHAR(n)
            case "VARCHAR": // VARCHAR(n)
            case "TEXT":
            case "LONGTEXT": // it is not mentioned in the Flink document
                return new VarCharType(VarCharType.MAX_LENGTH); // STRING
            case "BINARY":
            case "VARBINARY":
            case "BLOB":
                return new VarBinaryType(VarBinaryType.MAX_LENGTH); // BYTES
            default:
                log.error("Unsupported MySQL data type:" + fieldType);
                throw new UnsupportedDataTypeException("Unsupported MySQL data type:" + fieldType);
        }
    }

    /**
     * Convert DMDB compatibles with Mysql type to Flink type
     *
     * @see <a href="https://eco.dameng.com/document/dm/zh-cn/pm/dm_sql-introduction.html">DM_SQL 所支持的数据类型</a>
     * @see <a href="https://eco.dameng.com/document/dm/zh-cn/pm/jdbc-rogramming-guide.html">数据类型扩展</a>
     * @see <a href="https://nutz.cn/yvr/t/7piehdm6mmhubpmrsonva6a1fe">达梦数据库的集成（支持oracle、mysql兼容模式）</a>
     * @param fieldFormat DMDB for MySQL field format
     * @return Flink SQL Field Type
     */
    private LogicalType convertDmdbForMysqlType(FieldFormat fieldFormat) {
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
            case "INTEGER": // DMDB
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
                return formatDecimalType(precision, scale);
            case "BOOLEAN": // TINYINT(1)
                return new BooleanType();
            case "DATE":
                return new DateType();
            case "TIME": // TIME [(p)]
                return formatTimeType(precision); // TIME [(p)] [WITHOUT TIMEZONE]
            case "DATETIME": // DATETIME [(p)]
                return formatTimestampType(precision); // TIMESTAMP [(p)] [WITHOUT TIMEZONE]
            case "CHAR": // CHAR(n)
            case "VARCHAR": // VARCHAR(n)
            case "TEXT":
                return new VarCharType(VarCharType.MAX_LENGTH); // STRING
            case "BINARY":
            case "VARBINARY":
            case "BLOB":
                return new VarBinaryType(VarBinaryType.MAX_LENGTH); // BYTES
            default:
                log.error("Unsupported DMDB for MySQL data type:" + fieldType);
                throw new UnsupportedDataTypeException("Unsupported DMDB for MySQL data type:" + fieldType);
        }
    }

    /**
     * Convert DMDB compatibles with Oracle type to Flink type
     *
     * @param fieldFormat DMDB for Oracle Field Type
     * @return Flink SQL Field Type
     */
    private LogicalType convertDmdbForOracleType(FieldFormat fieldFormat) {
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
            case "DECIMAL":
                return formatDecimalType(precision, scale);
            case "INT": // DMDB
            case "INTEGER": // DMDB
                return new IntType();
            case "DATE":
                return new DateType();
            case "TIMESTAMP": // TODO: TIMESTAMP [(p)] [WITHOUT TIMEZONE]
            case "DATETIME": // DMDB
                return formatTimestampType(precision); // TIMESTAMP [(p)] [WITHOUT TIMEZONE]
            case "CHAR": // CHAR(n)
            case "VARCHAR": // VARCHAR(n)
            case "VARCHAR2": // it is not mentioned in the Flink document
            case "NVARCHAR2": // it is not mentioned in the Flink document
            case "CLOB":
            case "TEXT": // DMDB
                return new VarCharType(VarCharType.MAX_LENGTH);
            case "RAW": // RAW(s)
            case "BLOB":
            case "VARBINARY": // DMDB
                return new VarBinaryType(VarBinaryType.MAX_LENGTH);
            default:
                log.error("Unsupported DMDB for Oracle data type:" + fieldType);
                throw new UnsupportedDataTypeException("Unsupported DMDB for Oracle data type:" + fieldType);
        }
    }

    /**
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
                return formatDecimalType(precision, scale);
            case "DATE":
                return new DateType();
            case "TIMESTAMP": // TODO: TIMESTAMP [(p)] [WITHOUT TIMEZONE]
                return formatTimestampType(precision); // TIMESTAMP [(p)] [WITHOUT TIMEZONE]
            case "CHAR": // CHAR(n)
            case "VARCHAR": // VARCHAR(n)
            case "VARCHAR2": // it is not mentioned in the Flink document
            case "NVARCHAR2": // it is not mentioned in the Flink document
            case "CLOB":
                return new VarCharType(VarCharType.MAX_LENGTH);
            case "RAW": // RAW(s)
            case "BLOB":
                return new VarBinaryType(VarBinaryType.MAX_LENGTH);
            default:
                log.error("Unsupported Oracle data type:" + fieldType);
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
                log.error("Unsupported Oracle CDC data type:" + fieldType);
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
  private LogicalType convertLakehouseMixedIcebergType(FieldFormat fieldFormat) {
      String fieldType = StringUtils.upperCase(fieldFormat.getType());
      Integer precision = fieldFormat.getPrecision();
      Integer scale = fieldFormat.getScale();
      switch (fieldType) {
          case "STRING":
              return new VarCharType(VarCharType.MAX_LENGTH);
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
              return formatDecimalType(precision, scale);
          case "DATE":
              return new DateType();
          case "TIMESTAMP":
              return new TimestampType(TimestampType.DEFAULT_PRECISION); // TIMESTAMP(6)
          case "TIMESTAMPTZ":
              return new LocalZonedTimestampType(TimestampType.DEFAULT_PRECISION); // TIMESTAMP(6) WITH LOCAL TIME ZONE
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
              log.error("Unsupported Lakehouse data type:" + fieldType);
              throw new UnsupportedDataTypeException("Unsupported Lakehouse data type:" + fieldType);
        }
  }

    // ~ format LogicalType -------------------------------

    private DecimalType formatDecimalType(Integer precision, Integer scale) {
        boolean precisionRange = precision != null &&
                precision >= DecimalType.MIN_PRECISION && precision <= DecimalType.MAX_PRECISION;
        if (precisionRange && scale != null) {
            return new DecimalType(precision, scale);
        } else if (precisionRange) {
            return new DecimalType(precision);
        } else {
            return new DecimalType();
        }
    }

    private TimestampType formatTimestampType(Integer precision) {
        boolean precisionRange = precision != null
                && precision >= TimestampType.MIN_PRECISION && precision >= TimestampType.MAX_PRECISION;
        if (precisionRange) {
            return new TimestampType(precision);
        }
        return new TimestampType();
    }

    private TimeType formatTimeType(Integer precision) {
        boolean precisionRange = precision != null
                && precision >= TimeType.MIN_PRECISION && precision >= TimeType.MAX_PRECISION;
        if (precisionRange) {
            return new TimeType(precision);
        }
        return new TimeType();
    }
}

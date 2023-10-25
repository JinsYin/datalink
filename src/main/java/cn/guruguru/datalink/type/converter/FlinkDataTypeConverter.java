package cn.guruguru.datalink.type.converter;

import cn.guruguru.datalink.exception.UnsupportedDataSourceException;
import cn.guruguru.datalink.exception.UnsupportedDataTypeException;
import cn.guruguru.datalink.protocol.field.DataType;
import cn.guruguru.datalink.protocol.node.extract.cdc.KafkaNode;
import cn.guruguru.datalink.protocol.node.extract.cdc.MysqlCdcNode;
import cn.guruguru.datalink.protocol.node.extract.cdc.OracleCdcNode;
import cn.guruguru.datalink.protocol.node.extract.scan.DmScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.GreenplumScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.MySqlScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.OracleScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.PostgresqlScanNode;
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
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

@Slf4j
public class FlinkDataTypeConverter implements DataTypeConverter<String> {

    /**
     * Derive the engine type for the given datasource field type
     *
     * <pre>org.apache.inlong.sort.formats.base.TableFormatUtils#deriveLogicalType(FormatInfo)</pre>
     * @param nodeType node type
     * @param dataType source field
     */
    @Override
    public String toEngineType(String nodeType, DataType dataType) {
        switch (nodeType) {
            // Format -------------------
            case KafkaNode.TYPE: // JSON, CSV and so on
                return dataType.getType();
            // Scan -------------------
            case MySqlScanNode.TYPE:
                return convertMysqlType(dataType).asSummaryString();
            case OracleScanNode.TYPE:
                return convertOracleType(dataType).asSummaryString();
            case DmScanNode.TYPE:
                return convertDmdbForOracleType(dataType).asSummaryString();
            case PostgresqlScanNode.TYPE:
            case GreenplumScanNode.TYPE:
                return convertPostgresqlType(dataType).asSummaryString();
            case LakehouseLoadNode.TYPE:
                // Lakehouse table may be created by Spark, so it is necessary to convert the lakehouse type to the Flink type
                // e.g. Spark `TIMESTAMP` -> Arctic `TIMESTAMPTZ` -> Flink `TIMESTAMP(6) WITH LOCAL TIME ZONE`
                return convertLakehouseMixedIcebergType(dataType);
            // CDC --------------------
            case MysqlCdcNode.TYPE:
                return convertMysqlCdcType(dataType).asSummaryString();
            case OracleCdcNode.TYPE:
                return convertOracleCdcType(dataType).asSummaryString();
            default:
                throw new UnsupportedDataSourceException("Unsupported data source type:" + nodeType);
        }
    }

    // ~ scan type converter ------------------------------

    /**
     * Convert Mysql type to Flink type
     *
     * @see <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/jdbc/#data-type-mapping">Data Type Mapping</a>
     * @param dataType mysql field format
     * @return Flink SQL Field Type
     */
    private LogicalType convertMysqlType(DataType dataType) {
        String fieldType = StringUtils.upperCase(dataType.getType());
        Integer precision = dataType.getPrecision();
        Integer scale = dataType.getScale();
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
            case "TIMESTAMP": // it is not mentioned in the Flink document
                return formatTimestampType(precision); // TIMESTAMP [(p)] [WITHOUT TIMEZONE]
            case "CHAR": // CHAR(n)
            case "VARCHAR": // VARCHAR(n)
            case "TEXT":
            case "LONGTEXT": // it is not mentioned in the Flink document
            case "MEDIUMTEXT": // it is not mentioned in the Flink document
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
     * @param dataType DMDB for MySQL field format
     * @return Flink SQL Field Type
     */
    private LogicalType convertDmdbForMysqlType(DataType dataType) {
        String fieldType = StringUtils.upperCase(dataType.getType());
        Integer precision = dataType.getPrecision();
        Integer scale = dataType.getScale();
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
     * @param dataType DMDB for Oracle Field Type
     * @return Flink SQL Field Type
     */
    private LogicalType convertDmdbForOracleType(DataType dataType) {
        String fieldType = StringUtils.upperCase(dataType.getType());
        Integer precision = dataType.getPrecision();
        Integer scale = dataType.getScale();
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
            case "BIGINT": // DMDB
                return new BigIntType();
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
     * @param dataType Oracle Field Type
     * @return Flink SQL Field Type
     */
    private LogicalType convertOracleType(DataType dataType) {
        String fieldType = StringUtils.upperCase(dataType.getType());
        Integer precision = dataType.getPrecision();
        Integer scale = dataType.getScale();
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
     * Convert PostgreSQL/Greenplum type to Flink type
     *
     * @see <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/jdbc/#data-type-mapping">Data Type Mapping</a>
     * @param dataType
     * @return
     */
    private LogicalType convertPostgresqlType(DataType dataType) {
        String fieldType = StringUtils.upperCase(dataType.getType());
        Integer precision = dataType.getPrecision();
        Integer scale = dataType.getScale();
        switch (fieldType) {
            case "SMALLINT":
            case "INT2":
            case "SMALLSERIAL":
            case "SERIAL2":
                return new SmallIntType();
            case "INTEGER":
            case "SERIAL":
            case "INT4": // it is not mentioned in the Flink document
                return new IntType();
            case "BIGINT":
            case "BIGSERIAL":
            case "INT8": // it is not mentioned in the Flink document
                return new BigIntType();
            case "REAL":
            case "FLOAT4":
                return new FloatType();
            case "FLOAT8":
            case "DOUBLE PRECISION": // ?
                return new DoubleType();
            case "NUMERIC": // NUMERIC(p, s)
            case "DECIMAL": // NUMERIC(p, s)
                return formatDecimalType(precision, scale);
            case "BOOLEAN":
                return new BooleanType();
            case "DATE":
                return new DateType();
            case "TIME":
                return formatTimeType(precision); // TIME [(p)] [WITHOUT TIMEZONE]
            case "TIMESTAMP": // TODO: TIMESTAMP [(p)] [WITHOUT TIMEZONE]
                return formatTimestampType(precision); // TIMESTAMP [(p)] [WITHOUT TIMEZONE]
            case "CHAR": // CHAR(n)
            case "CHARACTER": // CHARACTER(n)
            case "VARCHAR": // VARCHAR(n)
            case "CHARACTER VARYING": // CHARACTER VARYING(n)
            case "TEXT":
                return new VarCharType(VarCharType.MAX_LENGTH);
            case "BYTEA":
                return new VarBinaryType(VarBinaryType.MAX_LENGTH);
            case "ARRAY": // TODO
                log.info("Combined PostgreSQL/Greenplum data type:" + fieldType);
            default:
                log.error("Unsupported PostgreSQL/Greenplum data type:" + fieldType);
                throw new UnsupportedDataTypeException("Unsupported PostgreSQL/Greenplum data type:" + fieldType);
        }
    }

    /**
    * Convert Arctic mixed iceberg type to Flink type
    *
    * @see <a href="https://arctic.netease.com/ch/flink/flink-ddl/#mixed-iceberg-data-types">Mixed Iceberg Data Types</a>
    * @see <a href="https://github.com/DTStack/chunjun/blob/master/chunjun-connectors/chunjun-connector-arctic/src/main/java/com/dtstack/chunjun/connector/arctic/converter/ArcticRawTypeMapper.java">ArcticRawTypeMapper</a>
    * @param dataType Arctic Mixed Iceberg Field Type
    * @return Flink SQL Field Type
    */
    private String convertLakehouseMixedIcebergType(DataType dataType) {
        String fieldType = StringUtils.upperCase(dataType.getType());
        Integer precision = dataType.getPrecision();
        Integer scale = dataType.getScale();
        switch (fieldType) {
            case "STRING":
                return new VarCharType(VarCharType.MAX_LENGTH).asSummaryString();
            case "BOOLEAN":
                return new BooleanType().asSummaryString();
            case "INT":
                return new IntType().asSummaryString();
            case "LONG":
                return new BigIntType().asSummaryString();
            case "FLOAT":
                return new FloatType().asSummaryString();
            case "DOUBLE":
                return new DoubleType().asSummaryString();
            case "DECIMAL": // DECIMAL(p, s)
                // return formatDecimalType(precision, scale).asSummaryString();
            case "DATE":
                return new DateType().asSummaryString();
            case "TIMESTAMP":
                return new TimestampType(TimestampType.DEFAULT_PRECISION).asSummaryString(); // TIMESTAMP(6)
            case "TIMESTAMPTZ":
                return new LocalZonedTimestampType(TimestampType.DEFAULT_PRECISION).asSummaryString(); // TIMESTAMP(6) WITH LOCAL TIME ZONE
            case "FIXED": // FIXED(p)
                // return new VarBinaryType(precision).asSummaryString(); // BINARY(p)
            case "UUID":
                return new VarBinaryType(16).asSummaryString(); // BINARY(16)
            case "BINARY": // TODO ?
                return new VarBinaryType(precision).asSummaryString();
            case "ARRAY":
            case "MAP":
            case "STRUCT":
                log.info("Combined Lakehouse data type:" + fieldType);
                return fieldType;
            default:
                log.info("Unconsidered Lakehouse data type:" + fieldType);
                return fieldType;
        }
    }

    // ~ cdc type converter -------------------------------

    /**
     * Convert Mysql CDC type to Flink type
     *
     * @see <a href="https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html#data-type-mappingg">Data Type Mapping</a>
     * @param dataType mysql field format
     * @return Flink SQL Field Type
     */
    private LogicalType convertMysqlCdcType(DataType dataType) {
        String fieldType = StringUtils.upperCase(dataType.getType());
        Integer precision = dataType.getPrecision();
        Integer scale = dataType.getScale();
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
            case "TIMESTAMP": // MySQL CDC
                return formatTimestampType(precision); // TIMESTAMP [(p)] [WITHOUT TIMEZONE]
            case "CHAR": // CHAR(n)
            case "VARCHAR": // VARCHAR(n)
            case "TEXT":
            case "LONGTEXT": // it is not mentioned in the Flink document
            case "MEDIUMTEXT": // it is not mentioned in the Flink document
                return new VarCharType(VarCharType.MAX_LENGTH); // STRING
            case "BINARY":
            case "VARBINARY":
            case "BLOB":
                return new VarBinaryType(VarBinaryType.MAX_LENGTH); // BYTES
            default:
                log.error("Unsupported MySQL CDC data type:" + fieldType);
                throw new UnsupportedDataTypeException("Unsupported MySQL CDC data type:" + fieldType);
        }
    }

    /**
     * Convert Oracle CDC type to Flink type
     *
     * @see <a href="https://ververica.github.io/flink-cdc-connectors/master/content/connectors/oracle-cdc.html#data-type-mapping">Data Type Mapping</a>
     * @param dataType Oracle CDC Field Type
     * @return Flink SQL Field Type
     */
    private LogicalType convertOracleCdcType(DataType dataType) {
        String fieldType = StringUtils.upperCase(dataType.getType());
        Integer precision = dataType.getPrecision();
        Integer scale = dataType.getScale();
        switch (fieldType) {
            case "NUMBER":
                if (scale == null) scale = 0; // Prevent mathematical operations on the null
                if (precision == null || precision == 0) {
                    return new DecimalType();
                }
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
                    return formatDecimalType(precision - scale, 0);
                } else if (scale <= 0 && precision - scale > 38) { // NUMBER(p, s <= 0), p - s > 38
                    return new VarCharType(VarCharType.MAX_LENGTH);
                } else if (scale > 0) { // NUMBER(p, s > 0)
                    return formatDecimalType(precision, scale);
                } else {
                    return formatDecimalType(precision, scale); // it is not mentioned in the Flink document
                }
            case "FLOAT":
            case "BINARY_FLOAT":
                return new FloatType();
            case "DOUBLE PRECISION":
            case "BINARY_DOUBLE":
                return new DecimalType();
            case "DATE":
            case "TIMESTAMP": // TIMESTAMP [(p)]
                return formatTimestampType(precision); // TIMESTAMP [(p)] [WITHOUT TIMEZONE]
            case "TIMESTAMP [(p)] WITH TIME ZONE": // TODO
                return formatZonedTimestampType(precision); // TIMESTAMP [(p)] WITH TIME ZONE
            case "TIMESTAMP [(p)] WITH LOCAL TIME ZONE": // TODO
                return formatLocalZonedTimestampType(precision); // TIMESTAMP_LTZ [(p)]
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


    // ~ format LogicalType -------------------------------

    private DecimalType formatDecimalType(Integer precision, Integer scale) {
        boolean precisionRange = precision != null
                && precision >= DecimalType.MIN_PRECISION
                && precision <= DecimalType.MAX_PRECISION;
        if (precisionRange && scale != null) {
            return new DecimalType(precision, scale);
        } else if (precisionRange) {
            return new DecimalType(precision);
        } else {
            return new DecimalType();
        }
    }

    private TimeType formatTimeType(Integer precision) {
        boolean precisionRange = precision != null
                && precision >= TimeType.MIN_PRECISION
                && precision >= TimeType.MAX_PRECISION;
        if (precisionRange) {
            return new TimeType(precision);
        }
        return new TimeType();
    }

    private TimestampType formatTimestampType(Integer precision) {
        boolean precisionRange = precision != null
                && precision >= TimestampType.MIN_PRECISION
                && precision >= TimestampType.MAX_PRECISION;
        if (precisionRange) {
            return new TimestampType(precision);
        }
        return new TimestampType();
    }

    private ZonedTimestampType formatZonedTimestampType(Integer precision) {
        boolean precisionRange = precision != null
                && precision >= ZonedTimestampType.MIN_PRECISION
                && precision >= ZonedTimestampType.MAX_PRECISION;
        if (precisionRange) {
            return new ZonedTimestampType(precision);
        }
        return new ZonedTimestampType();
    }

    private LocalZonedTimestampType formatLocalZonedTimestampType(Integer precision) {
        boolean precisionRange = precision != null
                && precision >= LocalZonedTimestampType.MIN_PRECISION
                && precision >= LocalZonedTimestampType.MAX_PRECISION;
        if (precisionRange) {
            return new LocalZonedTimestampType(precision);
        }
        return new LocalZonedTimestampType();
    }
}

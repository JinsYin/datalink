package cn.guruguru.datalink.type.converter;

import cn.guruguru.datalink.exception.UnsupportedDataSourceException;
import cn.guruguru.datalink.exception.UnsupportedDataTypeException;
import cn.guruguru.datalink.protocol.field.DataType;
import cn.guruguru.datalink.protocol.node.extract.scan.DmScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.GreenplumScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.MySqlScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.OracleScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.PostgresqlScanNode;
import cn.guruguru.datalink.protocol.node.load.LakehouseLoadNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;

@Slf4j
public class SparkDataTypeConverter implements DataTypeConverter<String> { // DataTypeConverter<DataType>

    private static final long serialVersionUID = 3025705873795163603L;

    /**
     * Converts to Spark data types
     *
     * @see <a href="https://spark.apache.org/docs/3.1.1/sql-ref-datatypes.html">Supported Data Types</a>
     * @param nodeType node type
     * @param dataType source field
     */
    @Override
    public String toEngineType(String nodeType, DataType dataType) {
        switch (nodeType) {
            // Scan ---------------------------------------
            case MySqlScanNode.TYPE:
                return convertMysqlType(dataType).simpleString().toUpperCase();
            case OracleScanNode.TYPE:
                return convertOracleType(dataType).simpleString().toUpperCase();
            case DmScanNode.TYPE:
                return convertDmdbForOracleType(dataType).simpleString().toUpperCase();
            case PostgresqlScanNode.TYPE:
            case GreenplumScanNode.TYPE:
                return convertPostgresqlType(dataType).simpleString().toUpperCase();
            case LakehouseLoadNode.TYPE:
                // Lakehouse table may be created by Spark, so it is necessary to convert the lakehouse type to the Flink type
                // e.g. Spark `TIMESTAMP` -> Arctic `TIMESTAMPTZ` -> Flink `TIMESTAMP(6) WITH LOCAL TIME ZONE`
                return convertLakehouseMixedIcebergType(dataType);
            // Other --------------------------------------
            default:
                throw new UnsupportedDataSourceException("Unsupported data source type:" + nodeType);
        }
    }

  // ~ scan type converter ------------------------------

  /**
   * Convert Mysql type to Flink type
   *
   * @see <a href="https://spark.apache.org/docs/3.1.1/sql-ref-datatypes.html">Data Types</a>
   * @see <a
   *     href="https://www.alibabacloud.com/help/zh/analyticdb-for-mysql/user-guide/spark-sql-syntax-for-creating-tables#section-rci-gfv-p71">数据类型映射</a>
   * @param dataType mysql field format
   * @return Flink SQL Field Type
   */
  private org.apache.spark.sql.types.DataType convertMysqlType(DataType dataType) {
        String fieldType = StringUtils.upperCase(dataType.getType());
        Integer precision = dataType.getPrecision();
        Integer scale = dataType.getScale();
        switch (fieldType) {
            case "TINYINT":
                return DataTypes.ByteType;
            case "SMALLINT":
            case "TINYINT UNSIGNED":
                return DataTypes.ShortType;
            case "INT":
            case "MEDIUMINT":
            case "SMALLINT UNSIGNED":
                return DataTypes.IntegerType;
            case "BIGINT":
            case "INT UNSIGNED":
            case "BIGINT UNSIGNED":
                return DataTypes.LongType;
            case "FLOAT":
                return DataTypes.FloatType;
            case "DOUBLE":
            case "DOUBLE PRECISION":
                return DataTypes.DoubleType;
            case "NUMERIC": // NUMERIC(p, s)
            case "DECIMAL": // DECIMAL(p, s)
                return formatDecimalType(precision, scale);
            case "BOOLEAN": // TINYINT(1)
            case "BIT":
                return DataTypes.BooleanType;
            case "DATE":
                return DataTypes.DateType;
            case "DATETIME": // DATETIME [(p)]
            case "TIMESTAMP":
                return DataTypes.TimestampType;
            case "CHAR": // CHAR(n)
            case "VARCHAR": // VARCHAR(n)
            case "TEXT":
            case "LONGTEXT":
            case "MEDIUMTEXT":
            case "TIME": // ?
                return DataTypes.StringType;
            case "BINARY":
            case "VARBINARY":
            case "BLOB":
            case "LONGBLOB":
                return DataTypes.BinaryType;
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
    private org.apache.spark.sql.types.DataType convertDmdbForMysqlType(DataType dataType) {
        String fieldType = StringUtils.upperCase(dataType.getType());
        Integer precision = dataType.getPrecision();
        Integer scale = dataType.getScale();
        switch (fieldType) {
            case "TINYINT":
                return DataTypes.ByteType;
            case "SMALLINT":
            case "TINYINT UNSIGNED":
                return DataTypes.ShortType;
            case "INT":
            case "INTEGER": // DMDB
            case "MEDIUMINT":
            case "SMALLINT UNSIGNED":
                return DataTypes.IntegerType;
            case "BIGINT":
            case "INT UNSIGNED": // TODO
            case "BIGINT UNSIGNED": // TODO
                return DataTypes.LongType;
            case "FLOAT":
                return DataTypes.FloatType;
            case "DOUBLE":
            case "DOUBLE PRECISION":
                return DataTypes.DoubleType;
            case "NUMERIC": // NUMERIC(p, s)
            case "DECIMAL": // DECIMAL(p, s)
                return formatDecimalType(precision, scale);
            case "BOOLEAN": // TINYINT(1)
                return DataTypes.BooleanType;
            case "DATE":
                return DataTypes.DateType;
            case "DATETIME": // DATETIME [(p)]
                return DataTypes.TimestampType;
            case "CHAR": // CHAR(n)
            case "VARCHAR": // VARCHAR(n)
            case "TEXT":
            case "TIME": // TIME [(p)] ???
                return DataTypes.StringType;
            case "BINARY":
            case "VARBINARY":
            case "BLOB":
                return DataTypes.BinaryType;
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
    private org.apache.spark.sql.types.DataType convertDmdbForOracleType(DataType dataType) {
        String fieldType = StringUtils.upperCase(dataType.getType());
        Integer precision = dataType.getPrecision();
        Integer scale = dataType.getScale();
        switch (fieldType) {
            case "BINARY_FLOAT":
                return DataTypes.FloatType;
            case "BINARY_DOUBLE":
                return DataTypes.DoubleType;
            case "SMALLINT":
            case "FLOAT": // FLOAT(s)
            case "DOUBLE PRECISION":
            case "REAL":
            case "NUMBER": // NUMBER(p, s)
            case "DECIMAL":
            case "NUMERIC":
                return formatDecimalType(precision, scale);
            case "INT": // DMDB
            case "INTEGER": // DMDB
                return DataTypes.IntegerType;
            case "BIGINT": // DMDB
                return DataTypes.LongType;
            case "DATE":
                return DataTypes.DateType;
            case "TIMESTAMP": // TODO: TIMESTAMP [(p)] [WITHOUT TIMEZONE]
            case "DATETIME": // DMDB
                return DataTypes.TimestampType;
            case "CHAR": // CHAR(n)
            case "VARCHAR": // VARCHAR(n)
            case "VARCHAR2": // it is not mentioned in the Flink document
            case "NVARCHAR2": // it is not mentioned in the Flink document
            case "CLOB":
            case "TEXT": // DMDB
                return DataTypes.StringType;
            case "RAW": // RAW(s)
            case "BLOB":
            case "VARBINARY": // DMDB
                return DataTypes.BinaryType;
            default:
                log.error("Unsupported DMDB for Oracle data type:" + fieldType);
                throw new UnsupportedDataTypeException("Unsupported DMDB for Oracle data type:" + fieldType);
        }
    }

    /**
     * Convert Oracle type to Spark type
     *
     * @see <a href="https://github.com/apache/spark/blob/v3.1.1/sql/core/src/main/scala/org/apache/spark/sql/jdbc/OracleDialect.scala#L43-L89">OracleDialect</a>
     * @param dataType Oracle Field Type
     * @return Flink SQL Field Type
     */
    private org.apache.spark.sql.types.DataType convertOracleType(DataType dataType) {
        String fieldType = StringUtils.upperCase(dataType.getType());
        Integer precision = dataType.getPrecision();
        Integer scale = dataType.getScale();
        switch (fieldType) {
            case "BINARY_FLOAT":
                return DataTypes.FloatType;
            case "BINARY_DOUBLE":
                return DataTypes.DoubleType;
            case "SMALLINT":
            case "FLOAT": // FLOAT(s)
            case "DOUBLE PRECISION":
            case "REAL":
            case "NUMBER": // NUMBER(p, s)
                return formatDecimalType(precision, scale);
            case "DATE":
                return DataTypes.DateType;
            case "TIMESTAMP":
                return DataTypes.TimestampType;
            case "CHAR": // CHAR(n)
            case "VARCHAR": // VARCHAR(n)
            case "VARCHAR2":
            case "NVARCHAR2":
            case "CLOB":
                return DataTypes.StringType;
            case "RAW": // RAW(s)
            case "BLOB":
                return DataTypes.BinaryType;
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
    private org.apache.spark.sql.types.DataType convertPostgresqlType(DataType dataType) {
        String fieldType = StringUtils.upperCase(dataType.getType());
        Integer precision = dataType.getPrecision();
        Integer scale = dataType.getScale();
        switch (fieldType) {
            case "SMALLINT":
            case "INT2":
            case "SMALLSERIAL":
            case "SERIAL2":
                return DataTypes.ShortType;
            case "INTEGER":
            case "SERIAL":
            case "INT4":
                return DataTypes.IntegerType;
            case "BIGINT":
            case "BIGSERIAL":
            case "INT8":
                return DataTypes.LongType;
            case "REAL":
            case "FLOAT4":
                return DataTypes.FloatType;
            case "FLOAT8":
            case "DOUBLE PRECISION": // ?
                return DataTypes.DoubleType;
            case "NUMERIC": // NUMERIC(p, s)
            case "DECIMAL": // NUMERIC(p, s)
                return formatDecimalType(precision, scale);
            case "BOOLEAN":
                return DataTypes.BooleanType;
            case "DATE":
                return DataTypes.DateType;
            case "TIMESTAMP": // TIMESTAMP [(p)] [WITHOUT TIMEZONE]
                return DataTypes.TimestampType;
            case "CHAR": // CHAR(n)
            case "CHARACTER": // CHARACTER(n)
            case "VARCHAR": // VARCHAR(n)
            case "CHARACTER VARYING": // CHARACTER VARYING(n)
            case "TEXT":
            case "TIME": // ?
                return DataTypes.StringType;
            case "BYTEA":
                return DataTypes.BinaryType;
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
                return DataTypes.StringType.simpleString().toUpperCase();
            case "BOOLEAN":
                return DataTypes.BooleanType.simpleString().toUpperCase();
            case "INT":
                return DataTypes.IntegerType.simpleString().toUpperCase();
            case "LONG":
                return DataTypes.LongType.simpleString().toUpperCase();
            case "FLOAT":
                return DataTypes.FloatType.simpleString().toUpperCase();
            case "DOUBLE":
                return DataTypes.DoubleType.simpleString().toUpperCase();
            case "DECIMAL": // DECIMAL(p, s)
                return formatDecimalType(precision, scale).simpleString().toUpperCase();
            case "DATE":
                return DataTypes.DateType.simpleString().toUpperCase();
            case "TIMESTAMP":
            case "TIMESTAMPTZ":
                return DataTypes.TimestampType.simpleString().toUpperCase();
            case "FIXED": // FIXED(p)
            case "UUID":
            case "BINARY": // TODO ?
                return DataTypes.BinaryType.simpleString().toUpperCase();
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

    // ~ formats data types -------------------------------

    private DecimalType formatDecimalType(Integer precision, Integer scale) {
        boolean precisionRange = precision != null
                                 && precision >= 0
                                 && precision <= 38;
        if (precisionRange && scale != null) {
            return new DecimalType(precision, scale);
        } else if (precisionRange) {
            return new DecimalType(precision);
        } else {
            return new DecimalType();
        }
    }
}

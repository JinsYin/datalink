package cn.guruguru.datalink.datasource;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

/**
 * Data Source Type copied from DTStack Tair
 *
 * @see <a href="https://github.com/DTStack/Taier/blob/master/taier-common/src/main/java/com/dtstack/taier/common/enums/DataSourceTypeEnum.java">DataSourceTypeEnum</a>
 */
public enum DataSourceType {

    /** RDBMS */
    MySQL(1, "MySQL", null),
    MySQL8(1001, "MySQL8", null),
    MySQLPXC(98, "MySQL PXC", null),
    Polardb_For_MySQL(28, "PolarDB for MySQL8", null),
    Oracle(2, "Oracle", null),
    SQLServer(3, "SQL Server", null),
    SQLSERVER_2017_LATER(32, "SQL Server JDBC", null),
    PostgreSQL(4, "PostgreSQL", null),
    ADB_PostgreSQL(54, "AnalyticDB PostgreSQL", null),
    DB2(19, "DB2", null),
    DMDB_FOR_MySQL(35, "DMDB", "For MySQL"),
    RDBMS(5, "RDBMS", null),
    KINGBASE8(40, "KingbaseES8", null),

    HIVE1X(27, "Hive", "1.x"),
    HIVE2X(7, "Hive", "2.x"),
    HIVE3X(50, "Hive", "3.x"),
    SparkThrift2_1(45, "SparkThrift", null),
    MAXCOMPUTE(10, "Maxcompute", null),
    GREENPLUM6(36, "Greenplum", null),
    LIBRA(21, "GaussDB", null),
    GBase_8a(22, "GBase_8a", null),
    HDFS(6, "HDFS", "2.x"),
    HDFS_TBDS(60, "HDFS", "TBDS"),
    FTP(9, "FTP", null),
    IMPALA(29, "Impala", null),
    ClickHouse(25, "ClickHouse", null),
    TiDB(31, "TiDB", null),
    CarbonData(20, "CarbonData", null),
    Kudu(24, "Kudu", null),
    Kylin(58, "Kylin URL", "3.x"),
    HBASE(8, "HBase", "1.x"),
    HBASE2(39, "HBase", "2.x"),
    HBASE_TBDS(61, "HBase", "TBDS"),
    Phoenix4(30, "Phoenix", "4.x"),
    Phoenix5(38, "Phoenix", "5.x"),
    ES(11, "Elasticsearch", "5.x"),
    ES6(33, "Elasticsearch", "6.x"),
    ES7(46, "Elasticsearch", "7.x"),
    MONGODB(13, "MongoDB", null),
    REDIS(12, "Redis", null),
    S3(41, "S3", null),
    KAFKA_TBDS(62, "Kafka", "TBDS"),
    KAFKA(26, "Kafka", "1.x"),
    KAFKA_2X(37, "Kafka", "2.x"),
    KAFKA_09(18, "Kafka", "0.9"),
    KAFKA_10(17, "Kafka", "0.10"),
    KAFKA_11(14, "Kafka", "0.11"),
    EMQ(34, "EMQ", null),
    WEB_SOCKET(42, "WebSocket", null),
    VERTICA(43, "Vertica", null),
    SOCKET(44, "Socket", null),
    ADS(15, "AnalyticDB MySQL", null),
    Presto(48, "Presto", null),
    SOLR(53, "Solr", "7.x"),
    INFLUXDB(55, "InfluxDB", "1.x"),
    INCEPTOR(52, "Inceptor", null),
    AWS_S3(51, "AWS S3", null),
    OPENTSDB(56, "OpenTSDB", "2.x"),
    Doris_JDBC(57, "Doris", "0.14.x(jdbc)"),
    Kylin_Jdbc(23, "Kylin JDBC", "3.x"),
    OceanBase(49, "OceanBase", null),
    RESTFUL(47, "Restful", null),
    TRINO(59, "Trino", null),
    DORIS_HTTP(64, "Doris", "0.14.x"),
    DMDB_FOR_ORACLE(67, "DMDB", "For Oracle"),

    /*** new data types **/
    Amoro(100, "Amoro", null),
    DMDB(101, "DMDB", null),
    Kafka(102, "Kafka", "2.x"),
    Greenplum(103, "Greenplum", null),
    ;

    public static List<Integer> hadoopDirtyDataSource =
            Lists.newArrayList(
                    DataSourceType.HIVE1X.getVal(),
                    DataSourceType.HIVE2X.getVal(),
                    DataSourceType.HIVE3X.getVal(),
                    DataSourceType.SparkThrift2_1.getVal());

    private static final Table<String, String, DataSourceType> CACHE =
            HashBasedTable.create();

    static {
        for (DataSourceType value : DataSourceType.values()) {
            CACHE.put(
                    value.getDataType(),
                    Objects.isNull(value.getDataVersion())
                            ? StringUtils.EMPTY
                            : value.getDataVersion(),
                    value);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(DataSourceType.class);

    DataSourceType(Integer val, String dataType, String dataVersion) {
        this.val = val;
        this.dataType = dataType;
        this.dataVersion = dataVersion;
    }

    /**
     * 根据数据源类型和版本获取数据源枚举信息
     *
     * @param dataType data type
     * @param dataVersion data source vertion
     * @return DataSourceType
     */
    public static DataSourceType typeVersionOf(String dataType, String dataVersion) {
        if (StringUtils.isBlank(dataVersion)) {
            dataVersion = StringUtils.EMPTY;
        }
        DataSourceType value;
        if ((value = CACHE.get(dataType, dataVersion)) == null) {
            LOGGER.error(
                    "this dataType cannot find,dataType:{},dataVersion:{}", dataType, dataVersion);
            throw new IllegalArgumentException("data type code not found");
        }
        return value;
    }

    /**
     * 根据数据源val获取数据源枚举信息
     *
     * @param val data source id
     * @return DataSourceType
     */
    public static DataSourceType valOf(Integer val) {
        Objects.requireNonNull(val);
        for (DataSourceType value : DataSourceType.values()) {
            if (Objects.equals(value.getVal(), val)) {
                return value;
            }
        }
        LOGGER.error("can not find this dataTypeCode:{}", val);
        throw new IllegalArgumentException("data type code not found");
    }

    /** 数据源值 */
    private Integer val;
    /** 数据源类型 */
    private String dataType;
    /** 数据源版本(可为空) */
    private String dataVersion;

    public Integer getVal() {
        return val;
    }

    public void setVal(Integer val) {
        this.val = val;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(String dataVersion) {
        this.dataVersion = dataVersion;
    }

    @Override
    public String toString() {
        if (StringUtils.isNotBlank(dataVersion)) {
            return dataType + "-" + dataVersion;
        }
        return dataType;
    }

}

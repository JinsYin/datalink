package cn.guruguru.datalink.protocol.node.extract.cdc;

import cn.guruguru.datalink.datasource.NodeDataSource;
import cn.guruguru.datalink.datasource.DataSourceType;
import cn.guruguru.datalink.protocol.Metadata;
import cn.guruguru.datalink.protocol.enums.MetaKey;
import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.node.extract.CdcExtractNode;
import cn.guruguru.datalink.protocol.field.WatermarkField;
import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.inlong.common.enums.MetaField;
import org.apache.inlong.common.enums.RowKindEnum;
import org.apache.inlong.sort.protocol.enums.ExtractMode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MySQL CDC Node
 *
 * @see org.apache.inlong.sort.protocol.node.extract.MySqlExtractNode
 * @see <a href="https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html">MySQL CDC Connector</a>
 */
@EqualsAndHashCode(callSuper = true)
@JsonTypeName(MysqlCdcNode.TYPE)
@NodeDataSource(value = DataSourceType.MySQL)
@Data
@NoArgsConstructor
public class MysqlCdcNode extends AbstractCdcNode implements Metadata, Serializable {
    public static final String TYPE = "MysqlCdc";

    @JsonProperty("serverId")
    private Integer serverId;
    @JsonProperty("serverTimeZone")
    private String serverTimeZone;

    @JsonCreator
    public MysqlCdcNode(@JsonProperty("id") String id,
                        @JsonProperty("name") String name,
                        @JsonProperty("fields") List<DataField> fields,
                        @Nullable @JsonProperty("properties") Map<String, String> properties,
                        @Nullable @JsonProperty("watermarkField") WatermarkField watermarkField,
                        @Nonnull @JsonProperty("hostname") String hostname,
                        @Nullable @JsonProperty("port") Integer port,
                        @Nonnull @JsonProperty("username") String username,
                        @Nonnull @JsonProperty("password") String password,
                        @Nonnull @JsonProperty("database-name") String databaseName,
                        @Nonnull @JsonProperty("table-name") String tableName,
                        @Nullable @JsonProperty("primaryKey") String primaryKey,
                        @Nullable @JsonProperty("serverId") Integer serverId,
                        @Nullable @JsonProperty("serverTimeZone") String serverTimeZone) {
        super(id, name, fields, properties, watermarkField,
                hostname, port, username, password, databaseName, tableName, primaryKey);
        this.serverId = serverId;
        this.serverTimeZone = serverTimeZone;
    }

    @Override
    public boolean isVirtual(MetaKey metaKey) {
        return true;
    }

    @Override
    public Set<MetaKey> supportedMetaFields() {
        return EnumSet.of(MetaKey.PROCESS_TIME, MetaKey.TABLE_NAME, MetaKey.DATA_CANAL,
                MetaKey.DATABASE_NAME, MetaKey.OP_TYPE, MetaKey.OP_TS, MetaKey.IS_DDL,
                MetaKey.TS, MetaKey.SQL_TYPE, MetaKey.MYSQL_TYPE, MetaKey.PK_NAMES,
                MetaKey.BATCH_ID, MetaKey.UPDATE_BEFORE, MetaKey.DATA_BYTES_DEBEZIUM,
                MetaKey.DATA_DEBEZIUM, MetaKey.DATA_BYTES_CANAL, MetaKey.DATA, MetaKey.DATA_BYTES);
    }

    @Override
    public String genTableName() {
        return super.genTableName();
    }

    @Override
    public String getPrimaryKey() {
        return super.getPrimaryKey();
    }

    @Override
    public List<DataField> getPartitionFields() {
        return super.getPartitionFields();
    }

    @Override
    public Map<String, String> tableOptions() {
        Map<String, String> options = super.tableOptions();
        options.put("connector", "mysql-cdc");
        options.put("hostname", super.getHostname());
        if (super.getPort() != null) {
            options.put("port", super.getPort().toString());
        }
        options.put("username", super.getUsername());
        options.put("password", super.getPassword());
        options.put("database-name", super.getDatabaseName());
        options.put("table-name", super.getTableName());
        if (serverId != null) {
            options.put("server-id", serverId.toString());
        }
        if (serverTimeZone != null) {
            options.put("server-time-zone", serverTimeZone);
        }
        return options;
    }
}

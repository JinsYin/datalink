package cn.guruguru.datalink.protocol.node.extract.cdc;

import cn.guruguru.datalink.datasource.NodeDataSource;
import cn.guruguru.datalink.datasource.DataSourceType;
import cn.guruguru.datalink.protocol.Metadata;
import cn.guruguru.datalink.protocol.enums.MetaKey;
import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.field.WatermarkField;
import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Oracle Cdc Node
 *
 * @see org.apache.inlong.sort.protocol.node.extract.OracleExtractNode
 * @see <a href="https://ververica.github.io/flink-cdc-connectors/master/content/connectors/oracle-cdc.html">Oracle CDC Connector</a>
 */
@EqualsAndHashCode(callSuper = true)
@JsonTypeName(OracleCdcNode.TYPE)
@NodeDataSource(DataSourceType.Oracle)
@Data
@NoArgsConstructor
public class OracleCdcNode extends AbstractCdcNode implements Metadata, Serializable {

    public static final String TYPE = "OracleCdc";

    @JsonProperty("schemaName")
    private String schemaName;
    @JsonProperty("url")
    private String url; // `jdbc:oracle:thin:@{hostname}:{port}:{database-name}` for default

    @JsonCreator
    public OracleCdcNode(@JsonProperty("id") String id,
                         @JsonProperty("name") String name,
                         @JsonProperty("fields") List<DataField> fields,
                         @Nullable @JsonProperty("properties") Map<String, String> properties,
                         @Nullable @JsonProperty("watermarkField") WatermarkField watermarkField,
                         @Nonnull @JsonProperty("hostname") String hostname,
                         @Nullable @JsonProperty("port") Integer port,
                         @Nonnull @JsonProperty("username") String username,
                         @Nonnull @JsonProperty("password") String password,
                         @Nonnull @JsonProperty("databaseName") String databaseName,
                         @Nonnull @JsonProperty("schemaName") String schemaName,
                         @Nonnull @JsonProperty("tableName") String tableName,
                         @Nullable @JsonProperty("primaryKey") String primaryKey,
                         @Nullable @JsonProperty("url") String url) {
        super(id, name, fields, properties, watermarkField,
                hostname, port, username, password, databaseName, tableName, primaryKey);
        this.schemaName = Preconditions.checkNotNull(schemaName, "schemaName is null");
        this.url = url;
    }

    @Override
    public boolean isVirtual(MetaKey metaKey) {
        return true;
    }

    @Override
    public Set<MetaKey> supportedMetaFields() {
        return EnumSet.of(MetaKey.PROCESS_TIME, MetaKey.TABLE_NAME, MetaKey.DATABASE_NAME,
                MetaKey.SCHEMA_NAME, MetaKey.OP_TS, MetaKey.OP_TYPE, MetaKey.DATA, MetaKey.DATA_BYTES,
                MetaKey.DATA_CANAL, MetaKey.DATA_BYTES_CANAL, MetaKey.IS_DDL, MetaKey.TS,
                MetaKey.SQL_TYPE, MetaKey.ORACLE_TYPE, MetaKey.PK_NAMES);
    }

    @Override
    public Map<String, String> tableOptions() {
        Map<String, String> options = super.tableOptions();
        options.put("connector", "oracle-cdc");
        options.put("hostname", getHostname());
        if (super.getPort() != null) {
            options.put("port", getPort().toString());
        }
        options.put("username", getUsername());
        options.put("password", getPassword());
        options.put("database-name", String.format("%s", getDatabaseName()));
        options.put("schema-name", String.format("%s", getSchemaName()));
        options.put("table-name", String.format("%s", getTableName()));
        return options;
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
}

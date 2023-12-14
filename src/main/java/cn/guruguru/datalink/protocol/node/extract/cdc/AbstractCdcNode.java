package cn.guruguru.datalink.protocol.node.extract.cdc;

import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.field.WatermarkField;
import cn.guruguru.datalink.protocol.node.extract.CdcExtractNode;
import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor(force = true)
public abstract class AbstractCdcNode extends CdcExtractNode implements Serializable {
    private static final long serialVersionUID = 1268414865736441916L;

    @JsonProperty("hostname")
    private String hostname;
    @JsonProperty("port")
    private Integer port;
    @JsonProperty("username")
    private String username;
    @JsonProperty("password")
    private String password;
    // the database-name also supports regular expressions
    @JsonProperty("databaseName")
    private String databaseName;
    // The table-name also supports regular expressions, if there are multiple, separate them with '|'
    @JsonProperty("tableName")
    private String tableName;
    // if there are multiple, separate them with commas
    @JsonProperty("primaryKey")
    private String primaryKey;

    @JsonCreator
    public AbstractCdcNode(@JsonProperty("id") String id,
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
                        @Nullable @JsonProperty("primaryKey") String primaryKey) {
        super(id, name, fields, properties, watermarkField);
        this.hostname = Preconditions.checkNotNull(hostname, "hostname is null");
        this.port = port;
        this.username = Preconditions.checkNotNull(username, "username is null");
        this.password = Preconditions.checkNotNull(password, "password is null");
        this.databaseName = Preconditions.checkNotNull(databaseName, "database-name is null");
        this.tableName = Preconditions.checkNotNull(tableName, "table-name is null");
        this.primaryKey = primaryKey;
    }

    @Override
    public String genTableName() {
        return tableName;
    }

    @Override
    public String getPrimaryKey() {
        return primaryKey;
    }

    @Override
    public List<DataField> getPartitionFields() {
        return super.getPartitionFields();
    }
}

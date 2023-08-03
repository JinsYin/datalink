package cn.guruguru.datalink.protocol.node.extract.scan;

import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.node.extract.ScanExtractNode;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor(force = true)
public abstract class JdbcScanNode extends ScanExtractNode implements Serializable {

    @JsonProperty("url")
    @Nonnull
    private String url;
    @JsonProperty("username")
    private String username;
    @JsonProperty("password")
    private String password;
    @JsonProperty("tableName")
    @Nonnull
    private String tableName;
    @JsonProperty("primaryKey")
    private String primaryKey;
    // @JsonProperty("filterClause")
    // private String filterClause;

    @JsonCreator
    public JdbcScanNode(@JsonProperty("id") String id,
                         @JsonProperty("name") String name,
                         @JsonProperty("fields") List<DataField> fields,
                         @Nullable @JsonProperty("properties") Map<String, String> properties,
                         @Nonnull @JsonProperty("url") String url,
                         @JsonProperty("username") String username,
                         @JsonProperty("password") String password,
                         @Nonnull @JsonProperty("tableName") String tableName,
                         @Nullable @JsonProperty("primaryKey") String primaryKey) {
        super(id, name, fields, properties);
        this.url = url;
        this.username = username;
        this.password = password;
        this.tableName = tableName;
        this.primaryKey = primaryKey;
    }

    @Override
    public Map<String, String> tableOptions() {
        return super.tableOptions();
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

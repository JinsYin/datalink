package cn.guruguru.datalink.protocol.node.extract.scan;

import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.node.extract.ScanExtractNode;
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

/**
 * JDBC Scan Extract Node
 *
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/jdbc/">JDBC SQL Connector</a>
 */
@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor(force = true)
public abstract class JdbcScanNode extends ScanExtractNode implements Serializable {

    //@JsonProperty
    //private JdbcDialect dialect; // for converting data types

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
    private String primaryKey; // if there are multiple, separate them with commas
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
        Map<String, String> options = super.tableOptions();
        options.put("connector", "jdbc");
        options.put("url", url);
        options.put("username", username);
        options.put("password", password);
        options.put("table-name", String.format("%s", tableName));
        return options;
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

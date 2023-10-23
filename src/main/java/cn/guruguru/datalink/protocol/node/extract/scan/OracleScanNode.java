package cn.guruguru.datalink.protocol.node.extract.scan;

import cn.guruguru.datalink.datasource.NodeDataSource;
import cn.guruguru.datalink.datasource.DataSourceType;
import cn.guruguru.datalink.protocol.field.DataField;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Oracle Scan Node
 *
 * @see org.apache.inlong.sort.protocol.node.extract.OracleExtractNode
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/jdbc/#connector-options">Connector Options</a>
 */
@JsonTypeName(OracleScanNode.TYPE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@NodeDataSource(DataSourceType.Oracle)
public class OracleScanNode extends JdbcScanNode {

    private static final long serialVersionUID = -5521981462461235288L;

    public static final String TYPE = "OracleScan";

    @JsonCreator
    public OracleScanNode(@JsonProperty("id") String id,
                         @JsonProperty("name") String name,
                         @JsonProperty("fields") List<DataField> fields,
                         @Nullable @JsonProperty("properties") Map<String, String> properties,
                         @Nonnull @JsonProperty("url") String url,
                         @JsonProperty("username") String username,
                         @JsonProperty("password") String password,
                         @Nonnull @JsonProperty("tableName") String tableName,
                         @Nullable @JsonProperty("primaryKey") String primaryKey) {
        super(id, name, fields, properties, url, username, password, tableName, primaryKey);
    }
}

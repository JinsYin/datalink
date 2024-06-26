package cn.guruguru.datalink.protocol.node.extract.scan;

import cn.guruguru.datalink.datasource.DataSourceType;
import cn.guruguru.datalink.datasource.NodeDataSource;
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
 * Greenplum Scan Node
 */
@JsonTypeName(GreenplumScanNode.TYPE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@NodeDataSource(DataSourceType.GREENPLUM6)
public class GreenplumScanNode extends JdbcScanNode {
    private static final long serialVersionUID = -7338939879338406887L;
    public static final String TYPE = "GreenplumScan";

    @JsonCreator
    public GreenplumScanNode(@JsonProperty("id") String id,
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

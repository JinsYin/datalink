package cn.guruguru.datalink.protocol.node.extract.scan;

import cn.guruguru.datalink.datasource.NodeDataSource;
import cn.guruguru.datalink.datasource.DataSourceType;
import cn.guruguru.datalink.protocol.field.DataField;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * DMDB Scan Node
 *
 * @see <a href="https://nutz.cn/yvr/t/7piehdm6mmhubpmrsonva6a1fe">达梦数据库的集成（支持oracle、mysql兼容模式）</a>
 * @see <a href="https://blog.csdn.net/weixin_43389023/article/details/105475686">达梦数据库开启其他数据库兼容模式</a>
 */
@EqualsAndHashCode(callSuper = true)
@JsonTypeName(DmScanNode.TYPE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@NoArgsConstructor
@NodeDataSource(DataSourceType.DMDB_FOR_ORACLE)
public class DmScanNode extends JdbcScanNode {
    public static final String TYPE = "DmScan";

    private static final long serialVersionUID = -5521981462461235288L;

    // private CompatibleMode compatibleMode;

    @JsonCreator
    public DmScanNode(@JsonProperty("id") String id,
                      @JsonProperty("name") String name,
                      @JsonProperty("fields") List<DataField> fields,
                      @Nullable @JsonProperty("properties") Map<String, String> properties,
                      @Nonnull @JsonProperty("url") String url,
                      @JsonProperty("username") String username,
                      @JsonProperty("password") String password,
                      @Nonnull @JsonProperty("tableName") String tableName,
                      @Nullable @JsonProperty("primaryKey") String primaryKey) {
        // TODO: process the url with compatibleMode property
        super(id, name, fields, properties, url, username, password, tableName, primaryKey);
    }
}

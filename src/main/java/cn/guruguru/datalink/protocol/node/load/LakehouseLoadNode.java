package cn.guruguru.datalink.protocol.node.load;

import cn.guruguru.datalink.datasource.NodeDataSource;
import cn.guruguru.datalink.datasource.DataSourceType;
import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.node.LoadNode;
import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nonnull;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * Lakehouse Load Node
 *
 * @see https://arctic.netease.com/ch/flink/flink-ddl
 */
@Data
@NoArgsConstructor(force = true)
@EqualsAndHashCode(callSuper = true)
@JsonTypeName(LakehouseLoadNode.TYPE)
@NodeDataSource(DataSourceType.Lakehouse)
public class LakehouseLoadNode extends LoadNode {

    public static final String TYPE = "LakehouseLoad";

    @Nonnull
    @JsonProperty("url")
    private String url;

    @Nonnull
    @JsonProperty("catalog")
    private String catalog;

    @JsonProperty("database")
    private String database;

    @JsonProperty("table")
    private String table;

    @JsonCreator
    public LakehouseLoadNode(@Nonnull @JsonProperty("url") String url,
                             @Nonnull @JsonProperty("catalog") String catalog,
                             @JsonProperty("database") String database,
                             @JsonProperty("table") String table) {
        this.url = url;
        this.catalog = catalog;
        this.database = database;
        this.table = table;
    }

    @Override
    public Map<String, String> tableOptions() {
        return super.tableOptions();
    }

    @Override
    public String genTableName() {
        return table;
    }

    @Override
    public String getPrimaryKey() {
        return super.getPrimaryKey();
    }

    @Override
    public List<DataField> getPartitionFields() {
        return super.getPartitionFields();
    }

    public String getCreateCatalog() throws MalformedURLException {
        URL baseUrl = new URL(url);
        String metastoreUrl = new URL(baseUrl, catalog).toString();
        return String.format("CREATE CATALOG %s WITH (\n"
            + "  'type'='arctic',\n"
            + "  'metastore.url'='%s'\n"
            + "); ", catalog, metastoreUrl);
    }
}
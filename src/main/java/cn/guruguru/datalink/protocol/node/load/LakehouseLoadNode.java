package cn.guruguru.datalink.protocol.node.load;

import cn.guruguru.datalink.datasource.NodeDataSource;
import cn.guruguru.datalink.datasource.DataSourceType;
import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.field.WatermarkField;
import cn.guruguru.datalink.protocol.node.LoadNode;
import cn.guruguru.datalink.protocol.relation.FieldRelation;
import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * Lakehouse Load Node
 *
 * @see <a href="https://amoro.netease.com/docs/latest/flink-ddl/">Flink DDL</a>
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

//    @JsonProperty("primaryKey")
//    @Nullable
//    private String primaryKey;

//    @JsonProperty("partitionKey")
//    @Nullable
//    private String partitionKey;

    @JsonCreator
    public LakehouseLoadNode(@JsonProperty("id") String id,
                             @JsonProperty("name") String name,
                             @JsonProperty("fields") List<DataField> fields,
                             @JsonProperty("fieldRelations") List<FieldRelation> fieldRelations,
                             @Nullable @JsonProperty("filterClause") String filterClause,
                             @Nullable @JsonProperty("properties") Map<String, String> properties,
                             @Nonnull @JsonProperty("url") String url,
                             @Nonnull @JsonProperty("catalog") String catalog,
                             @JsonProperty("database") String database,
                             @JsonProperty("table") String table) {
        super(id, name, fields, fieldRelations, filterClause, properties);
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
        // TODO
        return String.format("%s`.`%s`.`%s", catalog, database, table);
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
package cn.guruguru.datalink.protocol.node;

import cn.guruguru.datalink.protocol.field.DataField;
import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Data node for loading.
 *
 * @see org.apache.inlong.sort.protocol.node.LoadNode
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({

})
@NoArgsConstructor
@Data
public abstract class LoadNode implements Node, Serializable {
    @JsonProperty("id")
    private String id;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("name")
    private String name;
    @JsonProperty("fields")
    private List<DataField> fields;
    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("properties")
    private Map<String, String> properties;
    /**
     * Filter clauses for Flink SQL, e.g. `WHERE age > 0 LIMIT 10`
     */
    @Nullable
    @JsonProperty("filterClause")
    private String filterClause;

    @JsonCreator
    public LoadNode(@JsonProperty("id") String id,
                    @JsonProperty("name") String name,
                    @JsonProperty("fields") List<DataField> fields,
                    @Nullable @JsonProperty("filterClause") String filterClause,
                    @Nullable @JsonProperty("properties") Map<String, String> properties) {
        this.id = Preconditions.checkNotNull(id, "id is null");
        this.name = name;
        this.fields = Preconditions.checkNotNull(fields, "fields is null");
        Preconditions.checkState(!fields.isEmpty(), "fields is empty");
        this.filterClause = filterClause;
        this.properties = properties;
    }
}

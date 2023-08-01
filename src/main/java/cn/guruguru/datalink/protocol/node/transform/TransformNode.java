package cn.guruguru.datalink.protocol.node.transform;

import cn.guruguru.datalink.protocol.DataField;
import cn.guruguru.datalink.protocol.node.Node;
import cn.guruguru.datalink.protocol.transformation.relation.FieldRelation;
import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.inlong.sort.protocol.enums.FilterStrategy;
import org.apache.inlong.sort.protocol.transformation.FilterFunction;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Data node for transforming.
 *
 * @see org.apache.inlong.sort.protocol.node.transform.TransformNode
 */
@JsonSubTypes({
        @JsonSubTypes.Type(value = TransformNode.class, name = "transform"), // InLong Sort: baseTransform
})
@Data
@NoArgsConstructor
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
public class TransformNode implements Node, Serializable {
    private static final long serialVersionUID = -1202158328274891592L;

    @JsonProperty("id")
    private String id;
    @JsonProperty("name")
    private String name;
    @JsonProperty("fields")
    private List<DataField> fields;
    @JsonProperty("filters")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<FilterFunction> filters;
    @JsonProperty("filterStrategy")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private FilterStrategy filterStrategy;

    @JsonCreator
    public TransformNode(@JsonProperty("id") String id,
                         @JsonProperty("name") String name,
                         @JsonProperty("fields") List<DataField> fields,
                         @JsonProperty("filters") List<FilterFunction> filters,
                         @JsonProperty("filterStrategy") FilterStrategy filterStrategy) {
        this.id = Preconditions.checkNotNull(id, "id is null");
        this.name = name;
        this.fields = Preconditions.checkNotNull(fields, "fields is null");
        Preconditions.checkState(!fields.isEmpty(), "fields is empty");
        this.filters = filters;
        this.filterStrategy = filterStrategy;
    }

    @JsonIgnore
    @Override
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
    public String genTableName() {
        return "transform_" + id; // InLong Sort: "tansform_" + id
    }
}

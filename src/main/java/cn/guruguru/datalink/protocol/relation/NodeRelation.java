package cn.guruguru.datalink.protocol.relation;

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;
import java.util.List;

/**
 * Data Node Relation
 *
 * @see org.apache.inlong.sort.protocol.transformation.relation.NodeRelation
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = NodeRelation.class, name = NodeRelation.TYPE) // InLong Sort: baseRelation
})
@Data
@NoArgsConstructor
public class NodeRelation implements Serializable {
    private static final long serialVersionUID = 5491943876653981952L;
    public static final String TYPE = "Map";

    @JsonProperty("inputs")
    private List<String> inputs;
    @JsonProperty("outputs")
    private List<String> outputs;

    /**
     * NodeRelation Constructor
     *
     * @param inputs The inputs is a list of input node id
     * @param outputs The outputs is a list of output node id
     */
    @JsonCreator
    public NodeRelation(@JsonProperty("inputs") List<String> inputs,
                        @JsonProperty("outputs") List<String> outputs) {
        this.inputs = Preconditions.checkNotNull(inputs, "inputs is null");
        Preconditions.checkState(!inputs.isEmpty(), "inputs is empty");
        this.outputs = Preconditions.checkNotNull(outputs, "outputs is null");
        Preconditions.checkState(!outputs.isEmpty(), "outputs is empty");
    }
}

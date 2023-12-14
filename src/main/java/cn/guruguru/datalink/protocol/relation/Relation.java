package cn.guruguru.datalink.protocol.relation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;

/**
 * Node relations and field relations
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Relation implements Serializable {
    private static final long serialVersionUID = 3939629718063925560L;

    // 节点位置（仅限画布模式）
    // private Optional<List<NodeLocation>> nodeLocations;

    @JsonProperty("nodeRelations")
    private List<NodeRelation> nodeRelations;

    @JsonProperty("fieldRelations")
    private List<FieldRelation> fieldRelations;
}

package cn.guruguru.datalink.protocol.transformation.relation;

import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;

/**
 * Node relations and field relations
 */
@Data
public class Relation implements Serializable {
    // 节点位置（仅限画布模式）
    // private Optional<List<NodeLocation>> nodeLocations;

    @JsonProperty("nodeRelations")
    private List<NodeRelation> nodeRelations;

    @JsonProperty("fieldRelations")
    private List<FieldRelation> fieldRelations;
}

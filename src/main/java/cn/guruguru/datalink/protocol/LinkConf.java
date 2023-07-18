package cn.guruguru.datalink.protocol;

import cn.guruguru.datalink.protocol.node.Node;
import org.apache.inlong.sort.protocol.transformation.relation.NodeRelation;

import java.util.List;

/**
 * Data nodes and their relations
 */
public class LinkConf {
    private List<Node> nodes;

    private List<NodeRelation> nodeRelations;

    // 节点位置（仅限画布模式）
    // private Optional<List<NodeLocation>> nodeLocations;
}

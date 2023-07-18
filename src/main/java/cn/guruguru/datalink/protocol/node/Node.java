package cn.guruguru.datalink.protocol.node;

import java.io.Serializable;

/**
 * Data Node
 *
 * @see org.apache.inlong.sort.protocol.node.Node
 */
public interface Node {
    /**
     * Data node for extracting.
     *
     * @see org.apache.inlong.sort.protocol.node.ExtractNode
     */
    abstract class ExtractNode implements Node, Serializable {
    }
}

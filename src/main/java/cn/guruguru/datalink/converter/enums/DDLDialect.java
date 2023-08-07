package cn.guruguru.datalink.converter.enums;

import cn.guruguru.datalink.protocol.node.extract.scan.MySqlScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.OracleScanNode;

public enum DDLDialect {
    Oracle(OracleScanNode.TYPE),
    MySQL(MySqlScanNode.TYPE),
    ;

    private final String nodeType;

    DDLDialect(String nodeType) {
        this.nodeType = nodeType;
    }
    public String getNodeType() {
        return nodeType;
    }
}

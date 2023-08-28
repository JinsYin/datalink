package cn.guruguru.datalink.converter.enums;

import cn.guruguru.datalink.protocol.node.extract.scan.DmScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.MySqlScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.OracleScanNode;

public enum JdbcDialect {
    Oracle(OracleScanNode.TYPE, true),
    DMDB(DmScanNode.TYPE, false),
    MySQL(MySqlScanNode.TYPE, false),
    ;

    private final String nodeType;
    private final boolean supported;

    JdbcDialect(String nodeType, boolean supported) {
        this.nodeType = nodeType;
        this.supported = supported;
    }
    public String getNodeType() {
        return nodeType;
    }

    public boolean isSupported() {
        return supported;
    }
}

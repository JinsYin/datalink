package cn.guruguru.datalink.utils;

import cn.guruguru.datalink.datasource.DataSourceType;
import cn.guruguru.datalink.datasource.NodeDataSource;
import cn.guruguru.datalink.protocol.node.ExtractNode;
import cn.guruguru.datalink.protocol.node.LoadNode;
import cn.guruguru.datalink.protocol.node.Node;
import cn.guruguru.datalink.protocol.node.extract.CdcExtractNode;
import cn.guruguru.datalink.protocol.node.extract.ScanExtractNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class NodeUtil {

    /**
     * Get all nodes and their data source type
     */
    public static Map<String, DataSourceType> getAllNodes() {
        return getAllNodes(Node.class);
    }

    /**
     * Get all extract nodes and their data source types
     */
    public static Map<String, DataSourceType> getAllExtractNodes() {
        return getAllNodes(ExtractNode.class);
    }

    /**
     * Get all scan extract nodes and their data source types
     */
    public static Map<String, DataSourceType> getAllScanExtractNodes() {
        return getAllNodes(ScanExtractNode.class);
    }

    /**
     * Get all cdc extract nodes and their data source types
     */
    public static Map<String, DataSourceType> getAllCdcExtractNodes() {
        return getAllNodes(CdcExtractNode.class);
    }

    /**
     * Get all load nodes and their data source types
     */
    public static Map<String, DataSourceType> getAllLoadNodes() {
        return getAllNodes(LoadNode.class);
    }

    /**
     * Get some nodes and their data source type
     *
     * @param etlNodeClass using `Node.class`, `ExtractNode.class` and so on
     */
    public static Map<String, DataSourceType> getAllNodes(Class<? extends Node> etlNodeClass) {
        Class<?> clazz;
        try {
            clazz = Class.forName(etlNodeClass.getName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        JsonSubTypes annotation = clazz.getAnnotation(JsonSubTypes.class);
        JsonSubTypes.Type[] types = annotation.value();
        Map<String, DataSourceType> nodeTypeAndDataSource = new LinkedHashMap<>();
        Arrays.stream(types).forEach(type -> {
            String nodeType = type.name();
            Class<?> nodeClass = type.value();
            NodeDataSource dsAnnotation = nodeClass.getAnnotation(NodeDataSource.class);
            if (dsAnnotation != null) {
                DataSourceType dataSourceType = dsAnnotation.value();
                nodeTypeAndDataSource.put(nodeType, dataSourceType);
            }
        });
        return nodeTypeAndDataSource;
    }
}

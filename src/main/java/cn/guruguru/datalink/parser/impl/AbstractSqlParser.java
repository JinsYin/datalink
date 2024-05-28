package cn.guruguru.datalink.parser.impl;

import cn.guruguru.datalink.parser.Parser;
import cn.guruguru.datalink.parser.result.ParseResult;
import cn.guruguru.datalink.protocol.Pipeline;
import cn.guruguru.datalink.protocol.Metadata;
import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.field.DataType;
import cn.guruguru.datalink.protocol.field.Field;
import cn.guruguru.datalink.protocol.field.MetaField;
import cn.guruguru.datalink.protocol.node.ExtractNode;
import cn.guruguru.datalink.protocol.node.LoadNode;
import cn.guruguru.datalink.protocol.node.Node;
import cn.guruguru.datalink.protocol.node.extract.CdcExtractNode;
import cn.guruguru.datalink.protocol.node.transform.TransformNode;
import cn.guruguru.datalink.protocol.relation.FieldRelation;
import cn.guruguru.datalink.protocol.relation.NodeRelation;
import cn.guruguru.datalink.type.converter.DataTypeConverter;
import cn.guruguru.datalink.type.definition.DataTypes;
import cn.guruguru.datalink.type.definition.DataTypesFactory;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public abstract class AbstractSqlParser implements Parser {

    public static final String SOURCE_MULTIPLE_ENABLE_KEY = "source.multiple.enable";
    protected final Set<String> hasParsedSet = new HashSet<>();
    protected List<String> setSqls = new ArrayList<>();
    protected final List<String> extractTableSqls = new ArrayList<>();
    protected final List<String> transformTableSqls = new ArrayList<>();
    protected final List<String> loadTableSqls = new ArrayList<>();
    protected final List<String> insertSqls = new ArrayList<>();

    /**
     * Specify a data type converter
     *
     * @return a data type converter associated with computing engine
     */
    protected abstract DataTypeConverter getTypeConverter();

    protected abstract ParseResult getParseResult(Pipeline pipeline);


    // ~ Entrypoint ---------------------------------------

    /**
     * Parser a {@link Pipeline}
     *
     * @see org.apache.inlong.sort.parser.impl.FlinkSqlParser#parse()
     */
    @Override
    public ParseResult parse(Pipeline pipeline) {
        Preconditions.checkNotNull(pipeline, "the pipeline is null");
        Preconditions.checkNotNull(pipeline.getId(), "id is null");
        Preconditions.checkNotNull(pipeline.getNodes(), "nodes is null");
        Preconditions.checkState(!pipeline.getNodes().isEmpty(), "nodes is empty");
        Preconditions.checkNotNull(pipeline.getRelation(), "relation is null");
        Preconditions.checkNotNull(pipeline.getRelation().getNodeRelations(), "node relations is null");
        Preconditions.checkState(!pipeline.getRelation().getNodeRelations().isEmpty(), "node relations is empty");
        Preconditions.checkNotNull(pipeline.getRelation().getFieldRelations(), "field relations is null");
        // Preconditions.checkState(!pipeline.getRelation().getFieldRelations().isEmpty(), "field relations is empty");
        log.info("start parse the Pipeline, id:{}", pipeline.getId());
        // Parse nodes and node relations
        parseNodeRelations(pipeline);
        // TODO: Parse field relations
        log.info("parse the Pipeline success, id:{}", pipeline.getId());
        // Parse Result
        return getParseResult(pipeline);
    }

    // ~ SET Commands -------------------------------------

    /**
     * Parser the configuration of the computing engine
     *
     * @param pipeline a {@link Pipeline}
     * @return s set of set statements
     */
    protected List<String> parseConfiguration(Pipeline pipeline) {
        List<String> setSqls = new ArrayList<>();
        if (pipeline.getProperties() != null) {
            pipeline.getProperties().forEach(
                    (key, value) -> {
                        // TODO: Check if the key and value are valid
                        key = key.trim();
                        value = value.trim();
                        String statement = String.format("SET %s=%s", key, value);
                        setSqls.add(statement);
                    });
        }
        return setSqls;
    }

    // ~ Node Relations -----------------------------------

    protected void parseNodeRelations(Pipeline pipeline) {
        // Parse nodes
        Map<String, Node> nodeMap = getNodeMap(pipeline);
        // Parse node relations
        Map<String, NodeRelation> nodeRelationMap = getNodeRelationMap(pipeline);
        // Parser node relations
        pipeline.getRelation().getNodeRelations().forEach(r -> {
            parseNodeRelation(r, nodeMap, nodeRelationMap);
        });
    }

    private Map<String, Node> getNodeMap(Pipeline pipeline) {
        Map<String, Node> nodeMap = new HashMap<>(pipeline.getNodes().size());
        pipeline.getNodes().forEach(s -> {
            Preconditions.checkNotNull(s.getId(), "node id is null");
            nodeMap.put(s.getId(), s);
        });
        return nodeMap;
    }

    private Map<String, NodeRelation> getNodeRelationMap(Pipeline pipeline) {
        Map<String, NodeRelation> nodeRelationMap = new HashMap<>();
        pipeline.getRelation().getNodeRelations().forEach(r -> {
            for (String output : r.getOutputs()) {
                nodeRelationMap.put(output, r);
            }
        });
        return nodeRelationMap;
    }

    /**
     * parse node relation
     * Here we only parse the output node in the relation,
     * and the input node parsing is achieved by parsing the dependent node parsing of the output node.
     *
     * @param relation Define relations between nodes, it also shows the data flow
     * @param nodeMap Store the mapping relation between node id and node
     * @param relationMap Store the mapping relation between node id and relation
     */
    protected void parseNodeRelation(NodeRelation relation, Map<String, Node> nodeMap,
                                   Map<String, NodeRelation> relationMap) {
        log.info("start parse node relation, relation:{}", relation);
        Preconditions.checkNotNull(relation, "relation is null");
        Preconditions.checkState(!relation.getInputs().isEmpty(),
                "relation must have at least one input node");
        Preconditions.checkState(!relation.getOutputs().isEmpty(),
                "relation must have at least one output node");
        relation.getOutputs().forEach(s -> {
            Preconditions.checkNotNull(s, "node id in outputs is null");
            Node outputNode = nodeMap.get(s);
            Preconditions.checkNotNull(outputNode, "can not find any node by node id " + s);
            parseInputNodes(relation, nodeMap, relationMap);
            parseSingleNode(outputNode, relation, nodeMap);
            // for Load node we need to generate insert sql
            if (outputNode instanceof LoadNode) {
                insertSqls.add(genLoadNodeInsertSql((LoadNode) outputNode, relation, nodeMap));
            }
        });
        log.info("parse node relation success, relation:{}", relation);
    }

    /**
     * parse the input nodes corresponding to the output node
     * @param relation Define relations between nodes, it also shows the data flow
     * @param nodeMap Store the mapping relation between node id and node
     * @param relationMap Store the mapping relation between node id and relation
     */
    private void parseInputNodes(NodeRelation relation, Map<String, Node> nodeMap,
                                 Map<String, NodeRelation> relationMap) {
        for (String upstreamNodeId : relation.getInputs()) {
            if (!hasParsedSet.contains(upstreamNodeId)) {
                Node upstreamNode = nodeMap.get(upstreamNodeId);
                Preconditions.checkNotNull(upstreamNode,
                        "can not find any node by node id " + upstreamNodeId);
                parseSingleNode(upstreamNode, relationMap.get(upstreamNodeId), nodeMap);
            }
        }
    }

    /**
     * Parse a single node and generate the corresponding sql
     *
     * @param node The abstract of extract, transform, load
     * @param relation Define relations between nodes, it also shows the data flow
     * @param nodeMap store the mapping relation between node id and node
     */
    private void parseSingleNode(Node node, NodeRelation relation, Map<String, Node> nodeMap) {
        if (hasParsedSet.contains(node.getId())) {
            log.warn("the node has already been parsed, node id:{}", node.getId());
            return;
        }
        if (node instanceof ExtractNode) {
            log.info("start parse node, node id:{}", node.getId());
            String sql = genCreateSql(node);
            log.info("node id:{}, create table sql:\n{}", node.getId(), sql);
            registerTableSql(node, sql);
            hasParsedSet.add(node.getId());
        } else {
            Preconditions.checkNotNull(relation, "relation is null");
            if (node instanceof LoadNode) {
                String createSql = genCreateSql(node);
                log.info("node id:{}, create table sql:\n{}", node.getId(), createSql);
                registerTableSql(node, createSql);
                hasParsedSet.add(node.getId());
            } else if (node instanceof TransformNode) {
                TransformNode transformNode = (TransformNode) node;
                Preconditions.checkNotNull(transformNode.getFieldRelations(),
                        "field relations is null");
                Preconditions.checkState(!transformNode.getFieldRelations().isEmpty(),
                        "field relations is empty");
                String createSql = genCreateSql(node);
                log.info("node id:{}, create table sql:\n{}", node.getId(), createSql);
                String selectSql = genTransformSelectSql(transformNode, relation, nodeMap);
                log.info("node id:{}, transform sql:\n{}", node.getId(), selectSql);
                registerTableSql(node, createSql + " AS\n" + selectSql);
                hasParsedSet.add(node.getId());
            }
        }
        log.info("parse node success, node id:{}", node.getId());
    }

    private String genTransformSelectSql(TransformNode transformNode, NodeRelation relation,
                                         Map<String, Node> nodeMap) {
        // parse base relation that one to one and generate the transform sql
        Preconditions.checkState(relation.getInputs().size() == 1,
                "simple transform only support one input node");
        Preconditions.checkState(relation.getOutputs().size() == 1,
                "join node only support one output node");
        return genSimpleSelectSql(transformNode, transformNode.getFieldRelations(), relation,
                transformNode.getFilterClause(), nodeMap);
    }

    private void registerTableSql(Node node, String sql) {
        if (node instanceof ExtractNode) {
            extractTableSqls.add(sql);
        } else if (node instanceof TransformNode) {
            transformTableSqls.add(sql);
        } else if (node instanceof LoadNode) {
            loadTableSqls.add(sql);
        } else {
            throw new UnsupportedOperationException("Only support [ExtractNode|TransformNode|LoadNode]");
        }
    }

    protected String genLoadSelectSql(
            LoadNode loadNode, NodeRelation relation,
            Map<String, Node> nodeMap) {
        // parse base relation that one to one and generate the select sql
        Preconditions.checkState(relation.getInputs().size() == 1,
                "simple transform only support one input node");
        Preconditions.checkState(relation.getOutputs().size() == 1,
                "join node only support one output node");
        return genSimpleSelectSql(loadNode, loadNode.getFieldRelations(), relation,
                loadNode.getFilterClause(), nodeMap);
    }

    /**
     * Generate the most basic conversion sql one-to-one
     *
     * @param node The load node
     * @param fieldRelations The relation between fields
     * @param relation Define relations between nodes, it also shows the data flow
     * @param filterClause The filter clause
     * @param nodeMap Store the mapping relation between node id and node
     * @return Select sql for this node logic
     */
    protected String genSimpleSelectSql(Node node, List<FieldRelation> fieldRelations,
                                      NodeRelation relation, String filterClause,
                                      Map<String, Node> nodeMap) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        Map<String, FieldRelation> fieldRelationMap = new HashMap<>(fieldRelations.size());
        fieldRelations.forEach(s -> {
            fieldRelationMap.put(s.getOutputField().getName(), s);
        });
        parseFieldRelations(node.getNodeType(), node.getPrimaryKey(), node.getFields(), fieldRelationMap, sb);
        String tableName = nodeMap.get(relation.getInputs().get(0)).genTableName();
        sb.append("\n    FROM ").append(tableName).append(" ");
        parseFilterFields(filterClause, sb);
        return sb.toString();
    }

    /**
     * Parse filter fields to generate filter sql like 'where 1=1...'
     *
     * @param filterClause The filter clause
     * @param sb Container for storing sql
     */
    private void parseFilterFields(String filterClause, StringBuilder sb) {
        if (StringUtils.isNotBlank(filterClause)) {
            sb.append("\n ");
            sb.append(filterClause);
        }
    }

    /**
     * Parse field relation
     *
     * @param nodeType node type
     * @param primaryKey The primary key
     * @param fields The fields defined in node
     * @param fieldRelationMap The field relation map
     * @param sb Container for storing sql
     */
    protected abstract void parseFieldRelations(String nodeType,
                                     String primaryKey,
                                     List<DataField> fields,
                                     Map<String, FieldRelation> fieldRelationMap,
                                     StringBuilder sb);

    protected void castFiled(StringBuilder sb,
                             String nodeType,
                             DataType dataType,
                             String sourceColumn,
                             String targetColumn) {
        String targetType = getTypeConverter().toEngineType(nodeType, dataType);
        sb.append("\n    CAST(").append(sourceColumn).append(" as ").append(targetType).append(")")
                .append(" AS ").append(targetColumn).append(",");
    }

    protected void castAndCoalesceField(StringBuilder sb,
                                        String nodeType,
                                        Field inputField,
                                        DataField outputField) {
        String sourceIdentifier = inputField.format();
        String targetIdentifier = outputField.format();
        String targetType = getTypeConverter().toEngineType(nodeType, outputField.getDataType());
        String castSourceColumn = "CAST(" + sourceIdentifier + " as " + targetType + ")";
        DataTypes dataTypes = DataTypesFactory.of(getEngineType());
        sb.append("\n    COALESCE(").append(castSourceColumn).append(", ")
                .append(dataTypes.getDefaultValue(outputField.getDataType())).append(")")
                .append(" AS ").append(targetIdentifier).append(",");
    }

    /**
     * Generate create sql
     *
     * @param node The abstract of extract, transform, load
     * @return The creation sql pf table
     */
    protected abstract String genCreateSql(Node node);

    /**
     * Generate generic create sql
     *
     * @param node The abstract of extract, transform, load
     * @return The creation sql pf table
     */
    protected String genGenericCreateSql(Node node) {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        sb.append(node.genTableName()).append("(\n");
        String filterPrimaryKey = getFilterPrimaryKey(node);
        sb.append(parseFields(node.getFields(), node, filterPrimaryKey));
        sb.append(genPrimaryKey(node.getPrimaryKey(), filterPrimaryKey));
        if (node instanceof CdcExtractNode) {
            CdcExtractNode extractNode = (CdcExtractNode) node;
            if (extractNode.getWatermarkField() != null) {
                sb.append(",\n     ").append(extractNode.getWatermarkField().format());
            }
        }
        sb.append("\n)");
        if (node.getPartitionFields() != null && !node.getPartitionFields().isEmpty()) {
            sb.append(String.format(" PARTITIONED BY (%s)",
                    StringUtils.join(formatFields(node.getPartitionFields()), ",")));
        }
        sb.append(parseOptions(node.tableOptions(getEngineType())));
        return sb.toString();
    }

    /**
     * Generate load node insert sql
     *
     * @param loadNode The real data write node
     * @param relation The relation between nods
     * @param nodeMap The node map
     * @return Insert sql
     */
    protected String genLoadNodeInsertSql(LoadNode loadNode, NodeRelation relation, Map<String, Node> nodeMap) {
        Preconditions.checkNotNull(loadNode.getFieldRelations(), "field relations is null");
        Preconditions.checkState(!loadNode.getFieldRelations().isEmpty(),
                "field relations is empty");
        String selectSql = genLoadSelectSql(loadNode, relation, nodeMap);
        return "INSERT INTO " + loadNode.genTableName() + "\n    " + selectSql;
    }

    /**
     * Get filter PrimaryKey for Mongo when multi-sink mode
     *
     * @param node a node
     * @return a primary key
     */
    protected String getFilterPrimaryKey(Node node) {
        return null;
    }

    /**
     * Generate create transform sql
     *
     * @param node The transform node
     * @return The creation sql of transform node
     */
    protected String genCreateTransformSql(Node node) {
        return String.format("CREATE VIEW %s (%s)",
                node.genTableName(), parseTransformNodeFields(node.getFields()));
    }

    /**
     * Parse options to generate with options
     *
     * @param options The options defined in node
     * @return The with option string
     */
    protected abstract String parseOptions(Map<String, String> options);

    /**
     * Parse transform node fields
     *
     * @param fields The fields defined in node
     * @return Field formats in select sql
     */
    protected String parseTransformNodeFields(List<DataField> fields) {
        StringBuilder sb = new StringBuilder();
        for (DataField field : fields) {
            sb.append("\n    `").append(field.getName()).append("`,");
        }
        if (sb.length() > 0) {
            sb.delete(sb.lastIndexOf(","), sb.length());
        }
        return sb.toString();
    }

    /**
     * Parse fields
     *
     * @param fields The fields defined in node
     * @param node The abstract of extract, transform, load
     * @param filterPrimaryKey filter PrimaryKey, use for mongo
     * @return Field formats in select sql
     */
    protected String parseFields(List<DataField> fields, Node node, String filterPrimaryKey) {
        StringBuilder sb = new StringBuilder();
        String nodeType = node.getClass().getAnnotation(JsonTypeName.class).value();
        for (DataField field : fields) {
            if (StringUtils.isNotBlank(filterPrimaryKey) && field.getName().equals(filterPrimaryKey)) {
                continue;
            }
            sb.append("    `").append(field.getName()).append("` ");
            if (field instanceof MetaField) {
                if (!(node instanceof Metadata)) {
                    throw new IllegalArgumentException(String.format("Node: %s is not instance of Metadata",
                            node.getClass().getSimpleName()));
                }
                MetaField metaFieldInfo = (MetaField) field;
                Metadata metadataNode = (Metadata) node;
                if (!metadataNode.supportedMetaFields().contains(metaFieldInfo.getMetaKey())) {
                    throw new UnsupportedOperationException(String.format("Unsupported meta field for %s: %s",
                            metadataNode.getClass().getSimpleName(), metaFieldInfo.getMetaKey()));
                }
                sb.append(metadataNode.format(metaFieldInfo.getMetaKey()));
            } else {
                sb.append(getTypeConverter().toEngineType(nodeType, field.getDataType()));
            }
            if (StringUtils.isNotBlank(field.getComment())) {
                sb.append(" COMMENT '").append(field.getComment()).append("'");
            }
            sb.append(",\n");
        }
        if (sb.length() > 0) {
            sb.delete(sb.lastIndexOf(","), sb.length());
        }
        return sb.toString();
    }

    /**
     * Generate primary key format in sql
     *
     * @param primaryKey The primary key of table
     * @param filterPrimaryKey filter PrimaryKey, use for mongo
     * @return Primary key format in sql
     */
    protected abstract String genPrimaryKey(String primaryKey, String filterPrimaryKey);

    /**
     * Format fields with '`'
     *
     * @param fields The fields that need format
     * @return list of field after format
     */
    protected List<String> formatFields(String... fields) {
        List<String> formatFields = new ArrayList<>(fields.length);
        for (String field : fields) {
            if (!field.contains("`")) {
                formatFields.add(String.format("`%s`", field.trim()));
            } else {
                formatFields.add(field);
            }
        }
        return formatFields;
    }

    /**
     * Format fields with '`'
     *
     * @param fields a set of columns
     * @return a list of quoted columns
     */
    protected List<String> formatFields(List<DataField> fields) {
        List<String> formatFields = new ArrayList<>(fields.size());
        for (DataField field : fields) {
            formatFields.add(field.format());
        }
        return formatFields;
    }
}

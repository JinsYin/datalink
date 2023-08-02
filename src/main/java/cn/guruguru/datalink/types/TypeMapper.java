package cn.guruguru.datalink.types;

import cn.guruguru.datalink.formats.FieldFormat;
import cn.guruguru.datalink.protocol.node.ExtractNode;

/**
 * Type mapper interface
 */
public interface TypeMapper {
    /**
     * Converts DataSource DDL to computing engine DDL
     *
     * @param extractNode extract node
     * @param ddl DataSource DDL
     * @return DDL of computing engine
     */
    String deriveEngineSql(ExtractNode extractNode, String ddl);

    /**
     * Converts field type
     *
     * @see org.apache.inlong.sort.formats.base.TableFormatUtils#deriveLogicalType(FormatInfo)
     * @param extractNode extract node
     * @param fieldFormat source field
     * @return engine field
     */
    FieldFormat deriveEngineType(ExtractNode extractNode, FieldFormat fieldFormat);
}

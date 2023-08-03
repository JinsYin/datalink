package cn.guruguru.datalink.converter;

import cn.guruguru.datalink.protocol.field.FieldFormat;
import cn.guruguru.datalink.protocol.node.ExtractNode;

import java.io.Serializable;

/**
 * Type converter interface
 */
public interface TypeConverter extends Serializable {
    /**
     * Converts field type
     *
     * @see org.apache.inlong.sort.formats.base.TableFormatUtils#deriveLogicalType(FormatInfo)
     * @param extractNode extract node
     * @param fieldFormat source field
     * @return engine field
     */
    FieldFormat toEngineType(ExtractNode extractNode, FieldFormat fieldFormat);
}

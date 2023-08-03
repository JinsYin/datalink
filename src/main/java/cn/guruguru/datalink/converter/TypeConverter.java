package cn.guruguru.datalink.converter;

import cn.guruguru.datalink.protocol.field.FieldFormat;

import java.io.Serializable;

/**
 * Type converter interface
 */
public interface TypeConverter extends Serializable {
    /**
     * Converts field type
     *
     * @see org.apache.inlong.sort.formats.base.TableFormatUtils#deriveLogicalType(FormatInfo)
     * @param nodeType node type
     * @param fieldFormat source field
     * @return engine field
     */
    FieldFormat toEngineType(String nodeType, FieldFormat fieldFormat);
}

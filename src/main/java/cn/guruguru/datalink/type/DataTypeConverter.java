package cn.guruguru.datalink.type;

import cn.guruguru.datalink.protocol.field.FieldFormat;

import java.io.Serializable;

/**
 * Type converter interface
 */
public interface DataTypeConverter<T> extends Serializable {
    /**
     * Converts field type
     *
     * {@code org.apache.inlong.sort.formats.base.TableFormatUtils#deriveLogicalType(FormatInfo)}
     * @param nodeType node type
     * @param fieldFormat source field
     * @return engine field
     */
    T toEngineType(String nodeType, FieldFormat fieldFormat);
}

package cn.guruguru.datalink.type.converter;

import cn.guruguru.datalink.protocol.field.DataType;

import java.io.Serializable;

/**
 * Type converter interface
 */
public interface DataTypeConverter extends Serializable { // DataTypeConverter<T>

    /**
     * Converts field type
     *
     * {@code org.apache.inlong.sort.formats.base.TableFormatUtils#deriveLogicalType(FormatInfo)}
     * @param nodeType node type
     * @param dataType source field
     * @return engine field
     */
    String toEngineType(String nodeType, DataType dataType);
}

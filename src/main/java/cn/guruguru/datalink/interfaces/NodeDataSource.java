package cn.guruguru.datalink.interfaces;

import cn.guruguru.datalink.enums.DataSourceType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.ANNOTATION_TYPE, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface NodeDataSource {
    /**
     * Node data source type
     */
    DataSourceType value();
}

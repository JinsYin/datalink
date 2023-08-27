package cn.guruguru.datalink.datasource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.ANNOTATION_TYPE, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface NodeDataSource {
    /**
     * Node data source type
     *
     * @return DataSourceType
     */
    DataSourceType value();
}

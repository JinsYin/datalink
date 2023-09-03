package cn.guruguru.datalink.utils;

import com.google.common.base.Preconditions;

public class SqlUtil {
    /**
     * Compress a SQL
     *
     * @param sql regular SQL
     * @return compressed SQL
     */
    public static String compress(String sql) {
        Preconditions.checkNotNull(sql,"SQL is null");
        return sql.replaceAll("\\n", "")
                .replaceAll("\\(\\s*", "(")
                .replaceAll("\\s*\\)", ")")
                .replaceAll("\\s{2,}", " ")
                .replaceAll("\\s*;", ";")
                .trim();
    }
}

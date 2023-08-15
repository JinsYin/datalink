package cn.guruguru.datalink.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OracleUtil {

    final static public String ORACLE_URI_REGEX = "jdbc:oracle:thin:@(?://)?([.\\w]+):(\\d+)[:/](\\w+)";
    final static public Pattern ORACLE_URI_PATTERN = Pattern.compile(ORACLE_URI_REGEX);

    /**
     * 从 Oracle JDBC URL 解析获取主机名
     *
     * <p> SID 格式：jdbc:oracle:thin:@[HOST][:PORT]:SID
     * <p> ServiceName 格式：jdbc:oracle:thin:@//[HOST][:PORT]/SERVICE
     *
     * @see https://stackoverflow.com/a/12734630
     */
    public static String getHostFromUrl(String url) {
        Matcher matcher = ORACLE_URI_PATTERN.matcher(url);
        if (matcher.find()) {
            return matcher.group(1);
        }
        throw new IllegalArgumentException("The Oracle URI '" + url + "' is invalid");
    }

    /**
     * 从 Oracle JDBC URL 解析获取端口号
     */
    public static int getPortFromUrl(String url) {
        Matcher matcher = ORACLE_URI_PATTERN.matcher(url);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(2));
        }
        throw new IllegalArgumentException("The Oracle URI '" + url + "' is invalid");
    }

    /**
     * 从 Oracle JDBC URL 解析获取数据库（SID 或 ServiceName）
     */
    public static String getDatabaseFromUrl(String url) {
        Matcher matcher = ORACLE_URI_PATTERN.matcher(url);
        if (matcher.find()) {
            return matcher.group(3);
        }
        throw new IllegalArgumentException("The Oracle URI '" + url + "' is invalid");
    }
}

package cn.guruguru.datalink.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JdbcUrlUtil {

    final static public String ORACLE_URI_REGEX = "jdbc:oracle:thin:@(?://)?([.\\w]+):(\\d+)[:/](\\w+)";
    final static public Pattern ORACLE_URI_PATTERN = Pattern.compile(ORACLE_URI_REGEX);

    // ~ extractor for oracle ---------------------------------------

    /**
     * Extract hostname from an oracle jdbc url
     *
     * <p>SID format: {@code jdbc:oracle:thin:@[HOST][:PORT]:SID}
     * <p>ServiceName format: {@code jdbc:oracle:thin:@//[HOST][:PORT]/SERVICE}
     *
     * @see <a href="https://stackoverflow.com/a/12734630">How to parse a JDBC url to get hostname,port etc</a>
     * @param url Oracle JDBC URL
     * @return URL host
     */
    public static String extractHostFromOracleUrl(String url) {
        Matcher matcher = ORACLE_URI_PATTERN.matcher(url);
        if (matcher.find()) {
            return matcher.group(1);
        }
        throw new IllegalArgumentException("The Oracle URI '" + url + "' is invalid");
    }

    /**
     * Extract port from an oracle jdbc url
     *
     * @param url Oracle JDBC URL
     * @return URL port
     */
    public static int extractPortFromOracleUrl(String url) {
        Matcher matcher = ORACLE_URI_PATTERN.matcher(url);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(2));
        }
        throw new IllegalArgumentException("The Oracle URI '" + url + "' is invalid");
    }

    /**
     * Extract database(SID or ServiceName) from an oracle jdbc url
     *
     * @param url Oracle JDBC URL
     * @return  database name
     */
    public static String extractDatabaseFromOracleUrl(String url) {
        Matcher matcher = ORACLE_URI_PATTERN.matcher(url);
        if (matcher.find()) {
            return matcher.group(3);
        }
        throw new IllegalArgumentException("The Oracle URI '" + url + "' is invalid");
    }

    // ~ parser for mysql ------------------------------------------

}

package cn.guruguru.datalink.utils;

import cn.guruguru.datalink.ddl.table.JdbcDialect;
import cn.guruguru.datalink.exception.UnsupportedDataSourceException;
import lombok.extern.slf4j.Slf4j;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities for JDBC URL
 */
@Slf4j
public class JdbcUrlUtil {
    /**
     * Oracle URI
     * <p>SID format: {@code jdbc:oracle:thin:@[HOST][:PORT]:SID}
     * <p>ServiceName format: {@code jdbc:oracle:thin:@//[HOST][:PORT]/SERVICE}
     */
    public static final String ORACLE_URI_REGEX = "jdbc:oracle:thin:@(?://)?([.\\w]+):(\\d+)[:/](\\w+)";
    public static final String MYSQL_URI_REGEX = "jdbc:mysql://([.\\w]+):(\\d+)/(\\w+)";

    public static final Pattern ORACLE_URI_PATTERN = Pattern.compile(ORACLE_URI_REGEX);
    public static final Pattern MYSQL_URI_PATTERN = Pattern.compile(MYSQL_URI_REGEX);

    // ~ parser for common ------------------------------------------

    /**
     * Extract hostname from a jdbc url
     *
     * @see <a href="https://stackoverflow.com/a/12734630">How to parse a JDBC url to get hostname,port etc</a>
     * @param dialect JDBC dialect
     * @param url a JDBC URL
     * @return URL host
     */
    public static String extractHost(JdbcDialect dialect, String url) {
        Matcher matcher = getJdbcPattern(dialect).matcher(url);
        if (matcher.find()) {
            return matcher.group(1);
        }
        throw new IllegalArgumentException(String.format("The %s URI '%s' is invalid", dialect, url));
    }

    /**
     * Extract port from a jdbc url
     *
     * @param dialect JDBC dialect
     * @param url a JDBC URL
     * @return URL port
     */
    public static int extractPort(JdbcDialect dialect, String url) {
        Matcher matcher = getJdbcPattern(dialect).matcher(url);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(2));
        }
        throw new IllegalArgumentException(String.format("The %s URI '%s' is invalid", dialect, url));
    }

    /**
     * Extract database from a jdbc url
     *
     * @param dialect JDBC dialect
     * @param url a JDBC URL
     * @return database name
     */
    public static String extractDatabase(JdbcDialect dialect, String url) {
        Matcher matcher = getJdbcPattern(dialect).matcher(url);
        if (matcher.find()) {
            return matcher.group(3);
        }
        throw new IllegalArgumentException(String.format("The %s URI '%s' is invalid", dialect, url));
    }

    /**
     * Get a pattern for different JDBC dialect
     *
     * @param dialect JDBC dialect
     * @return a pattern
     */
    private static Pattern getJdbcPattern(JdbcDialect dialect) {
        switch (dialect) {
            case Oracle:
                return ORACLE_URI_PATTERN;
            case MySQL:
                return MYSQL_URI_PATTERN;
            default:
                log.error("Unsupported data source for parsing jdbc url");
                throw new UnsupportedDataSourceException("Unsupported data source for parsing jdbc url");
        }
    }

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
    public static int extractPort(String url) {
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
}

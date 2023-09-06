package cn.guruguru.datalink.protocol.enums;

import java.util.Locale;

/**
 * Kafka scan startup mode
 *
 * @see org.apache.inlong.sort.protocol.enums.KafkaScanStartupMode
 */
public enum KafkaScanStartupMode {

    EARLIEST_OFFSET("earliest-offset"),
    LATEST_OFFSET("latest-offset"),
    SPECIFIC_OFFSETS("specific-offsets"),
    TIMESTAMP_MILLIS("timestamp");

    KafkaScanStartupMode(String value) {
        this.value = value;
    }

    private final String value;

    public String getValue() {
        return value;
    }

    public static KafkaScanStartupMode forName(String name) {
        for (KafkaScanStartupMode startupMode : values()) {
            if (startupMode.getValue().equals(name.toLowerCase(Locale.ROOT))) {
                return startupMode;
            }
        }
        throw new IllegalArgumentException(String.format("Unsupported KafkaScanStartupMode:%s", name));
    }

}

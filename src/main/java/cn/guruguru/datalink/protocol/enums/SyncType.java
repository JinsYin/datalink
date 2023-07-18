package cn.guruguru.datalink.protocol.enums;

/**
 * Synchronization Type
 */
public enum SyncType {
    // 离线同步，包括出/入湖，支持单表或多表
    BATCH,

    // 实时同步，目前仅限入湖，支持单表和整库
    STREAM,
}
package cn.guruguru.datalink.protocol;


import cn.guruguru.datalink.protocol.enums.SyncType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Properties;

/**
 * Data structure for synchronization
 *
 * @see org.apache.inlong.sort.protocol.StreamInfo
 * @see org.apache.inlong.sort.protocol.GroupInfo
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class LinkInfo {
    // 开发模式（FORM/JSON/SQL/CANVAS）
    // private SyncMode syncMode;

    private SyncType syncType;

    private String syncName;

    private String syncDesc;

    // 任务配置（SQL 模式除外）
    private LinkConf linkConf;

    // 将自动转成 SET 语句
    private Properties flinkConf;
}


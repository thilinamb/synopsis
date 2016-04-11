package neptune.geospatial.ft;

import ds.funnel.topic.Topic;

import java.util.List;

/**
 * @author Thilina Buddhika
 */
public class BackupTopicInfo {
    private Topic primary;
    private List<TopicInfo> backups;

    public BackupTopicInfo(Topic primary, List<TopicInfo> backups) {
        this.primary = primary;
        this.backups = backups;
    }
}

package neptune.geospatial.ft;

import ds.funnel.topic.Topic;

import java.util.List;

/**
 * @author Thilina Buddhika
 */
public class BackupTopicInfo {
    private String stream;
    private Topic primary;
    private List<TopicInfo> backups;

    public BackupTopicInfo(String stream, Topic primary, List<TopicInfo> backups) {
        this.stream = stream;
        this.primary = primary;
        this.backups = backups;
    }

    public Topic getPrimary() {
        return primary;
    }

    public List<TopicInfo> getBackups() {
        return backups;
    }

    public String getStream() {
        return stream;
    }
}

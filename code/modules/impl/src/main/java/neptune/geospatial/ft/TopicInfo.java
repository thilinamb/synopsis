package neptune.geospatial.ft;

import ds.funnel.topic.Topic;

/**
 * @author Thilina Buddhika
 */
public class TopicInfo{

    private Topic topic;
    private String resourceEndpoint;

    public TopicInfo(Topic topic, String resourceEndpoint) {
        this.topic = topic;
        this.resourceEndpoint = resourceEndpoint;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TopicInfo topicInfo = (TopicInfo) o;

        return topic.equals(topicInfo.topic);
    }

    @Override
    public int hashCode() {
        return topic.hashCode();
    }
}

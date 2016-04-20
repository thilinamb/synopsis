package neptune.geospatial.ft;

import ds.funnel.data.format.FormatReader;
import ds.funnel.data.format.FormatWriter;
import ds.funnel.topic.StringTopic;
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

    public TopicInfo() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TopicInfo topicInfo = (TopicInfo) o;

        return topic.equals(topicInfo.topic);
    }

    public Topic getTopic() {
        return topic;
    }

    public String getResourceEndpoint() {
        return resourceEndpoint;
    }

    @Override
    public int hashCode() {
        return topic.hashCode();
    }

    public void marshall(FormatWriter formatWriter){
        formatWriter.writeString(topic.toString());
        formatWriter.writeString(resourceEndpoint);
    }

    public void unmarshall(FormatReader formatReader){
        this.topic = new StringTopic(formatReader.readString());
        this.resourceEndpoint = formatReader.readString();
    }
}

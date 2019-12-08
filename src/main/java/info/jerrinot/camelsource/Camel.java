package info.jerrinot.camelsource;

import com.hazelcast.jet.pipeline.StreamSource;
import info.jerrinot.camelsource.internal.KafkaConnectSources;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Properties;

public final class Camel {
    private Camel() {

    }

    public static StreamSource<SourceRecord> source(String name, Properties props) {
        Properties clonedProps = new Properties();
        clonedProps.putAll(props);
        clonedProps.setProperty("name", name);
        clonedProps.setProperty("connector.class", "org.apache.camel.kafkaconnector.CamelSourceConnector");
        return KafkaConnectSources.connect(clonedProps);
    }
}

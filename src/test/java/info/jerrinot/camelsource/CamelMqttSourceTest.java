package info.jerrinot.camelsource;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Pipeline;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.util.Properties;

import static info.jerrinot.camelsource.TestUtils.publishMessage;

public final class CamelMqttSourceTest {
    private static final String TOPIC = "topicName";

    @ClassRule
    public static GenericContainer broker
            = new GenericContainer("eclipse-mosquitto:1.6.8")
            .withExposedPorts(1883);

    @Test
    public void smokeTest() throws MqttException {
        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(Camel.source("camelSource", camelProps()))
                .withoutTimestamps()
                .map(record -> new String((byte[])record.value()))
                .drainTo(TestUtils.assertingSink("Hello World"));
        Job job = Jet.newJetInstance().newJob(pipeline);

        publishMessage(getBrokerUrl(), TOPIC, "Hello World");

        TestUtils.assertJobCompletedSuccessfully(job);
    }

    private static Properties camelProps() {
        Properties properties = new Properties();
        String camelUrl = "paho:" + TOPIC + "?brokerUrl=" + getBrokerUrl();
        properties.setProperty("camel.source.url", camelUrl);
        return properties;
    }

    private static String getBrokerUrl() {
        return "tcp://" + broker.getContainerIpAddress() + ":" + broker.getFirstMappedPort();
    }
}

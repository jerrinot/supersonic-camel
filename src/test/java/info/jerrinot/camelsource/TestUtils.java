package info.jerrinot.camelsource;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;
import java.util.concurrent.CompletionException;

import static org.junit.Assert.assertTrue;

public final class TestUtils {
    private TestUtils() {

    }

    public static void publishMessage(String brokerUrl, String topic, String message) throws MqttException {
        IMqttClient publisher = new MqttClient(brokerUrl, UUID.randomUUID().toString());
        publisher.connect();
        MqttMessage mqttMessage = new MqttMessage(message.getBytes());
        mqttMessage.setRetained(true);
        publisher.publish(topic, mqttMessage);
    }

    @NotNull
    public static Sink<String> assertingSink(String expectedMessage) {
        return AssertionSinks.assertCollectedEventually(60, l -> {
            if (!l.contains(expectedMessage)) {
                throw new AssertionError("Missing item");
            }
        } );
    }

    public static void assertJobCompletedSuccessfully(Job job) {
        try {
            job.join();
        } catch (CompletionException e) {
            assertTrue(e.getMessage().contains("Assertion passed successfully"));
        }
    }
}

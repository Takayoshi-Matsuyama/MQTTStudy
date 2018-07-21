package tech.tkys.iot.mqtt.study;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * The Main class of this application.
 */
public class Main {

    /**
     * Provides entry point of this application.
     * @param args - The command line arguments.
     */
    public static void main(String[] args) {
        int exitStatus = 0;
        try {
            executeMqttCommunication();
        } catch (Exception e) {
            e.printStackTrace();
            exitStatus = 1;
        } finally {
            System.exit(exitStatus);
        }
    }

    /**
     * Executes MQTT communication.
     */
    private static void executeMqttCommunication() {
        String brokerURI = "tcp://192.168.0.11:1883"; // Specifies an external broker.
//        String brokerURI = "tcp://localhost:1883";

        String topic = "MQTTTest";
        int qos = 2;    // Quality of Service (2: Exactly once delivery)

        try {
            MqttClient subscriberClient = subscribeToBroker(brokerURI, topic);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
            String messageString = String.format("Hello from MQTT Publisher (MacBook Pro): %s", sdf.format(new Date()));
            publishToBroker(brokerURI, topic, messageString, qos);

            if (subscriberClient != null) {
                subscriberClient.disconnect();
                System.out.println("[MQTT Subscriber] Disconnected");
            }
        } catch (MqttException e) {
            System.out.println("reason "+e.getReasonCode());
            System.out.println("msg "+e.getMessage());
            System.out.println("loc "+e.getLocalizedMessage());
            System.out.println("cause "+e.getCause());
            System.out.println("excep "+e);
            e.printStackTrace();
        }
    }

    /**
     * Subscribes to a MQTT broker.
     * @param brokerURI - URI specifies a broker.
     * @param topic     - MQTT topic to subscribe.
     * @return          - MQTT subscriber client object.
     * @throws MqttException
     */
    private static MqttClient subscribeToBroker(String brokerURI, String topic) throws MqttException {
        MqttClient client = new MqttClient(brokerURI, MqttClient.generateClientId());
        client.setCallback( new MyMqttCallback() );
        System.out.println("[MQTT Subscriber] Connecting to broker: " + brokerURI);
        client.connect();
        System.out.println("[MQTT Subscriber] Connected");
        System.out.println("[MQTT Subscriber] Subscribing to broker: " + brokerURI + ", topic: " + topic);
        client.subscribe("MQTTTest");
        System.out.println("[MQTT Subscriber] Subscribed");
        return client;
    }

    /**
     * Publishes a test message to a MQTT broker.
     * @param brokerURI     - URI specifies a broker.
     * @param topic         - MQTT topic to publish.
     * @param messageString - Message string to publish.
     * @param qos           - Quality of Service (0: At most once delivery, 1: At least once delivery, 2: Exactly once delivery)
     * @throws MqttException
     */
    private static void publishToBroker(String brokerURI, String topic, String messageString, int qos) throws MqttException {
        MemoryPersistence persistence = new MemoryPersistence();
        MqttClient publisherClient = new MqttClient(brokerURI, MqttClient.generateClientId(), persistence);

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);

        System.out.println("[MQTT Publisher] Connecting to broker: " + brokerURI);
        publisherClient.connect(connOpts);
        System.out.println("[MQTT Publisher] Connected");

        System.out.println("[MQTT Publisher] Publishing message: " + messageString);
        MqttMessage message = new MqttMessage(messageString.getBytes());
        message.setQos(qos);
        publisherClient.publish(topic, message);
        System.out.println("[MQTT Publisher] Message published");

        publisherClient.disconnect();
        System.out.println("[MQTT Publisher] Disconnected");
    }

    /**
     * Tests MQTT publishing behaviour using 'Getting Started' code of Eclipse Paho project.
     * http://www.eclipse.org/paho/clients/java/
     */
    private static void testPahoMqttClientPublish() throws MqttException {
        String topic        = "MQTT Examples";
        String content      = "Message from MqttPublishSample";
        int qos             = 2;
        String broker       = "tcp://iot.eclipse.org:1883";
        String clientId     = "JavaSample";

        MemoryPersistence persistence = new MemoryPersistence();
        MqttClient sampleClient = new MqttClient(broker, clientId, persistence);

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);

        System.out.println("Connecting to broker: "+broker);
        sampleClient.connect(connOpts);
        System.out.println("Connected");

        System.out.println("Publishing message: "+content);
        MqttMessage message = new MqttMessage(content.getBytes());
        message.setQos(qos);
        sampleClient.publish(topic, message);
        System.out.println("Message published");

        sampleClient.disconnect();
        System.out.println("Disconnected");
    }
}

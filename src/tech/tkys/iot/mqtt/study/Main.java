package tech.tkys.iot.mqtt.study;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class Main {

    public static void main(String[] args) {
	    System.out.println("MQTT Study");

	    String brokerURI = "tcp://localhost:1883";
	    String topic = "MQTTTest";
	    int qos = 2;    // Quality of Service (2: Exactly once delivery)

        try {
            MqttClient subscriberClient = subscribeToLocalBroker(brokerURI, topic);
            publishToLocalBroker(brokerURI, topic, "Hello from MQTT Publisher", qos);
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

    private static MqttClient subscribeToLocalBroker(String brokerURI, String topic) throws MqttException {
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
     * Publishes a test message to local MQTT broker.
     */
    private static void publishToLocalBroker(String brokerURI, String topic, String messageString, int qos) throws MqttException {
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
        System.exit(0);
    }
}

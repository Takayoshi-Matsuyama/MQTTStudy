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

        try {
            MqttClient subscriberClient = subscribeToLocalBroker();
            publishToLocalBroker();
            subscriberClient.disconnect();
        } catch (MqttException e) {
            System.out.println("reason "+e.getReasonCode());
            System.out.println("msg "+e.getMessage());
            System.out.println("loc "+e.getLocalizedMessage());
            System.out.println("cause "+e.getCause());
            System.out.println("excep "+e);
            e.printStackTrace();
        }
    }

    private static MqttClient subscribeToLocalBroker() throws MqttException {
        MqttClient client = new MqttClient("tcp://localhost:1883", MqttClient.generateClientId());
        client.setCallback( new MyMqttCallback() );
        client.connect();
        client.subscribe("MQTTTest");
        return client;
    }

    /**
     * Publishes a test message to local MQTT broker (tcp://localhost:1883).
     */
    private static void publishToLocalBroker() throws MqttException {
        String topic        = "MQTTTest";
        String content      = "Message from MqttPublisher";
        int qos             = 2;
        String broker       = "tcp://localhost:1883";
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

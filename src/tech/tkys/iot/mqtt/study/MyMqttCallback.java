package tech.tkys.iot.mqtt.study;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MyMqttCallback implements MqttCallback {

    @Override
    public void connectionLost(Throwable throwable) {
        System.out.println("MQTT Callback: Connection Lost.");
    }

    @Override
    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
        System.out.println("MQTT Callback: Message Arraived:\n\t"+ new String(mqttMessage.getPayload()));
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        System.out.println("MQTT Callback: Delivery Complete.");
    }
}

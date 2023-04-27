package org.example;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class XMLProducer {

    public static void main(String[] args) throws Exception {
        String kafkaServer = "localhost:9092";
        String topic = "my-xml-topic";

        // Set up Kafka producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create a Kafka producer instance
        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);

        // Create some XML data to send to Kafka
        String xmlData = "<person><name>GEETHA</name><age>19</age><address>MG Road</address></person>";

        // Send the XML data to the Kafka topic
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, xmlData);
        producer.send(record);

        // Close the Kafka producer
        producer.close();
    }
}

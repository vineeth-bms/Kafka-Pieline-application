package org.example;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class XMLMultiProducer {

    private static final String KAFKA_TOPIC = "my-ott-topic";

    public static void main(String[] args) throws IOException {
        final Logger log = LoggerFactory.getLogger(XMLConsumer.class);
        // Create Kafka producer properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create a Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Read the XML file
        File file = new File("/home/vineeth-hpe/mydata.xml");
        BufferedReader reader = new BufferedReader(new FileReader(file));

        // Send each row of data as a separate message to Kafka
        String line;
        while ((line = reader.readLine()) != null) {
            // Send the message to Kafka
            String message = line.trim();
            if (!message.isEmpty()) {
                producer.send(new ProducerRecord<>(KAFKA_TOPIC, null, message));
            }
        }

        // Close the Kafka producer
        producer.close();
        log.info("Records in xml file are sent ");
    }
}


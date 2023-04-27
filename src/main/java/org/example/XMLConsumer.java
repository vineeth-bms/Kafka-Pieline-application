package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;
import org.json.XML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class XMLConsumer {
    private static final Logger log = LoggerFactory.getLogger(XMLConsumer.class);
    private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/kafka_demo";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "9731397020Vin*";
    private static final String KAFKA_TOPIC = "my-xml-topic";

    public static void main(String[] args) {
        // Create Kafka consumer properties
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-kafka-group");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create a Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the Kafka topic
        consumer.subscribe(Collections.singletonList(KAFKA_TOPIC));

        // Create a connection to the MySQL database
        try (Connection connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD)) {
            while (true) {
                // Poll for new messages from Kafka
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String xml = record.value();

                    log.info("Key: " + record.key() + ", Value: " + record.value());

                    // Convert XML to JSON
                    JSONObject json = (JSONObject) XML.toJSONObject(xml);
                    log.info("Json string contains" + json);
                    log.info("person "+ json.get("person"));

                    // Insert JSON data into MySQL
                    String sql = "INSERT INTO person (address,name,age) VALUES (?,?,?)";
                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        JSONObject person = (JSONObject) json.get("person");
//                        String city = (String) address.get("city");
                        statement.setString(1, (String) person.get("address"));
                        statement.setString(2, (String) person.get("name"));
                        statement.setString(3, String.valueOf((Integer)person.get("age")));
//                        stmt.setString(1, json.getString("name"));
                        log.info("statement is "+ statement);
                        statement.executeUpdate();
                        log.info("DONE");

                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // Close the Kafka consumer
            consumer.close();
        }
    }
}

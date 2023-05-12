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
    private static final String KAFKA_TOPIC = "my-ott-topic";

    public static void main(String[] args) {
        // Create Kafka consumer properties
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-ott-group");
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
                    JSONObject json =  XML.toJSONObject(xml);
                    log.info("Json string contains" + json);
                    log.info("ott_demo "+ json.get("ott_demo"));

                    // Insert JSON data into MySQL
                    String sql = "INSERT INTO ott_data (FERT,F_CODE,DESCRIPTION,PID,QS,PTM,PUBLISH_DATE) VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY " +
                            "UPDATE DESCRIPTION=?, PID=?,QS=?, PTM=?, PUBLISH_DATE=?";
                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        JSONObject ott_data = (JSONObject) json.get("ott_demo");
//
                        statement.setString(1, (String) ott_data.get("fert"));
                        statement.setString(2, (String) ott_data.get("fcode"));
                        statement.setString(3, (String) ott_data.get("desc"));
                        statement.setString(4, (String) ott_data.get("pid"));
                        statement.setString(5, (String) ott_data.get("qs"));
                        statement.setString(6, (String) ott_data.get("ptm"));
                        statement.setString(7, (String) ott_data.get("date"));
                        statement.setString(8, (String) ott_data.get("desc"));  // set the new value for field2 (if the row exists)
                        statement.setString(9, (String) ott_data.get("pid"));   // set the new value for field2 (if the row exists)
                        statement.setString(10, (String) ott_data.get("qs"));    // set the new value for field2 (if the row exists)
                        statement.setString(11, (String) ott_data.get("ptm"));   // set the new value for field2 (if the row exists)
                        statement.setString(12, (String) ott_data.get("date"));  // set the new value for field2 (if the row exists)
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

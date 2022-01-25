package fr.viteducode.kafkaclient.producer.simple;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class NetflixProducerApplication {

    public static void main(String[] args) {

        String topic = args.length > 0 ? args[0] : "topic-netflix-content";

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        InputStream fileStream = NetflixProducerApplication.class.getClassLoader().getResourceAsStream("netflix_titles.csv");

        try (BufferedReader br = new BufferedReader(new InputStreamReader(fileStream))) {

            br.lines().skip(1).forEach(line -> {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, line);
                producer.send(record, (recordMetadata, exception) -> {
                    if (exception != null) {
                        System.out.println(exception.getMessage());
                    }
                });
            });

        } catch (IOException e) {
            System.exit(1);
        }


        producer.close();
    }
}

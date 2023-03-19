package fr.viteducode.kafkaclient.producer.simple;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import fr.viteducode.kafkaclient.producer.simple.data.BillionaireKey;
import fr.viteducode.kafkaclient.producer.simple.data.BillionaireValue;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class ForbesProducerApplication {

    private final static String TOPIC = "topic-billionaires";

    private final RFC4180Parser rfc4180ParserBuilder;

    private KafkaProducer<BillionaireKey, BillionaireValue> producer;

    public ForbesProducerApplication() {

        this.rfc4180ParserBuilder = new RFC4180ParserBuilder()
                .withFieldAsNull(CSVReaderNullFieldIndicator.EMPTY_SEPARATORS)
                .withSeparator(',')
                .build();

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        properties.put("schema.registry.url", "http://localhost:8081");

        producer = new KafkaProducer<>(properties);
    }

    public static void main(String[] args) {
        new ForbesProducerApplication().run();
    }

    public void run() {
        try (InputStream fileStream = ForbesProducerApplication.class.getClassLoader().getResourceAsStream("forbes_billionaires_2022.csv");
             InputStreamReader inputStreamReader = new InputStreamReader(fileStream);
             CSVReader csvReader = new CSVReaderBuilder(inputStreamReader).withCSVParser(rfc4180ParserBuilder).withSkipLines(1).build()) {

            String[] line;
            while ((line = csvReader.readNext()) != null) {

                BillionaireKey key = this.mapKey(line);
                BillionaireValue value = this.mapValue(line);

                ProducerRecord<BillionaireKey, BillionaireValue> record = new ProducerRecord<>(TOPIC, key, value);
                producer.send(record, (recordMetadata, exception) -> {
                    if (exception != null) {
                        System.out.println(exception.getMessage());
                    }
                });
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        producer.close();
    }

    private BillionaireValue mapValue(String[] line) {
        return BillionaireValue.newBuilder()
                .setName(line[1])
                .setAge(this.parseInt(line[2]))
                .setFinalWorth(this.parseInt(line[3]))
                .setCategory(line[4])
                .setSource(line[5])
                .setCountry(line[6])
                .setCountry(line[7])
                .setState(line[8])
                .build();
    }

    private BillionaireKey mapKey(String[] line) {
        return BillionaireKey.newBuilder()
                .setId(this.parseInt(line[0]))
                .build();
    }

    private Integer parseInt(String value) {
        Integer intValue = null;
        if (value != null) {
            intValue = Integer.parseInt(value);
        }
        return intValue;
    }
}

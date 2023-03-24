package fr.viteducode.kafkaclient.producer.simple;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import fr.viteducode.kafkaclient.producer.simple.data.BillionaireKey;
import fr.viteducode.kafkaclient.producer.simple.data.BillionaireValue;
import fr.viteducode.kafkaclient.producer.simple.data.CompanyKey;
import fr.viteducode.kafkaclient.producer.simple.data.CompanyValue;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class ForbesProducerApplication {

    private final static String TOPIC_BILLIONAIRE = "topic-billionaires";

    private final static String TOPIC_COMPANY = "topic-companies";

    private final RFC4180Parser rfc4180ParserBuilder;

    private final KafkaProducer<SpecificRecord, SpecificRecord> producer;

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
        this.produceBillionaire();
        this.produceCompanie();
        producer.close();
    }

    private void produceBillionaire() {

        try (InputStream fileStream = ForbesProducerApplication.class.getClassLoader().getResourceAsStream("forbes_billionaires_2022.csv");
             InputStreamReader inputStreamReader = new InputStreamReader(fileStream);
             CSVReader csvReader = new CSVReaderBuilder(inputStreamReader).withCSVParser(rfc4180ParserBuilder).withSkipLines(1).build()) {

            String[] line;
            while ((line = csvReader.readNext()) != null) {

                BillionaireKey key = this.mapBillionaireKey(line);
                BillionaireValue value = this.mapBillionaireValue(line);

                ProducerRecord<SpecificRecord, SpecificRecord> record = new ProducerRecord<>(TOPIC_BILLIONAIRE, key, value);
                producer.send(record, (recordMetadata, exception) -> {
                    if (exception != null) {
                        System.out.println(exception.getMessage());
                    }
                });
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void produceCompanie() {

        try (InputStream fileStream = ForbesProducerApplication.class.getClassLoader().getResourceAsStream("forbes_companies_2022.csv");
             InputStreamReader inputStreamReader = new InputStreamReader(fileStream);
             CSVReader csvReader = new CSVReaderBuilder(inputStreamReader).withCSVParser(rfc4180ParserBuilder).withSkipLines(1).build()) {

            String[] line;
            while ((line = csvReader.readNext()) != null) {

                CompanyKey key = this.mapCompanyKey(line);
                CompanyValue value = this.mapCompanyValue(line);

                ProducerRecord<SpecificRecord, SpecificRecord> record = new ProducerRecord<>(TOPIC_COMPANY, key, value);
                producer.send(record, (recordMetadata, exception) -> {
                    if (exception != null) {
                        System.out.println(exception.getMessage());
                    }
                });
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private BillionaireValue mapBillionaireValue(String[] line) {
        return BillionaireValue.newBuilder()
                .setName(line[1])
                .setAge(this.parseInt(line[2]))
                .setFinalWorth(this.parseInt(line[3]))
                .setCategory(line[4])
                .setOrganization(line[9])
                .setCountry(line[6])
                .setState(line[8])
                .build();
    }

    private CompanyValue mapCompanyValue(String[] line) {
        return CompanyValue.newBuilder()
                .setName(line[1])
                .setCountry(line[2])
                .setSales(line[3])
                .setProfit(line[4])
                .build();
    }

    private BillionaireKey mapBillionaireKey(String[] line) {
        return BillionaireKey.newBuilder()
                .setRank(this.parseInt(line[0]))
                .build();
    }

    private CompanyKey mapCompanyKey(String[] line) {
        return CompanyKey.newBuilder()
                .setRank(this.parseInt(line[0]))
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

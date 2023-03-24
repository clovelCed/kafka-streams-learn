package fr.viteducode.kafkastreams.dsl;

import fr.viteducode.kafkaclient.producer.simple.data.*;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class BillionaireWithCompanyStreamsApplication {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kstream-billionaires_with_companies-store");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-parsing-billionaire-with-company");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        properties.put("auto.offset.reset", "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<CompanyKey, CompanyValue> streamCompanies = builder.stream("topic-companies");

        KStream<CompanyNameKey, CompanyValue> streamCompanyByName = streamCompanies.
                selectKey(
                        (companyKey, companyValue) -> CompanyNameKey.newBuilder().setCompanyName(companyValue.getName()).build(),
                        Named.as("select-key-company-join")
                );

        KStream<BillionaireKey, BillionaireValue> streamBillionaires = builder.stream("topic-billionaires");

        KStream<CompanyNameKey, BillionaireValue> streamBillionaireByName = streamBillionaires
                .filter((billionaireKey, billionaireValue) -> billionaireValue.getOrganization() != null)
                .selectKey(
                        (billionaireKey, billionaireValue) -> CompanyNameKey.newBuilder().setCompanyName(billionaireValue.getOrganization()).build(),
                        Named.as("select-key-billionaire-join")
                );

        streamBillionaireByName.join(streamCompanyByName.toTable(), (billionaireValue, companyValue) -> {

                    return BillionaireWithCompanyValue
                            .newBuilder()
                            .setName(billionaireValue.getName())
                            .setAge(billionaireValue.getAge())
                            .setCountry(billionaireValue.getCountry())
                            .setFinalWorth(billionaireValue.getFinalWorth())
                            .setCompany(BillionaireCompanyValue
                                    .newBuilder()
                                    .setName(companyValue.getName())
                                    .build());

                }).map((companyNameKey, value) -> {

                    BillionaireWithCompanyKey key = BillionaireWithCompanyKey
                            .newBuilder()
                            .setBillionaireName(value.getName())
                            .setCompanyName(value.getCompany().getName())
                            .build();
                    return KeyValue.pair(key, value.build());
                })
                .to("topic-billionaire-with-company");

        Topology topology = builder.build();

        final CountDownLatch latch = new CountDownLatch(1);

        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaStreams.close();
            latch.countDown();
        }));

        try {
            kafkaStreams.start();
            latch.await();
        } catch (final Exception e) {
            System.exit(1);
        }
    }
}

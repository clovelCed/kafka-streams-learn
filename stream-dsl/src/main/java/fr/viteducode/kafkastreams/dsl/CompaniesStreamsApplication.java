package fr.viteducode.kafkastreams.dsl;

import fr.viteducode.kafkaclient.producer.simple.data.CompanyKey;
import fr.viteducode.kafkaclient.producer.simple.data.CompanyValue;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class CompaniesStreamsApplication {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kstream-companies-store");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-parsing-companies");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        properties.put("auto.offset.reset", "earliest");

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                "http://localhost:8081");
        final Serde<CompanyValue> valueSerde = new SpecificAvroSerde<>();
        valueSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<CompanyKey, CompanyValue> stream = builder.stream("topic-companies");

        stream
                .filter((CompanyKey, CompanyValue) -> CompanyValue.getCountry() != null)
                .groupBy((CompanyKey, companyValue) -> companyValue.getCountry(), Grouped.with(Serdes.String(), valueSerde))
                .count(Materialized.as("topic-store-count-companies-by-country"))
                .toStream()
                .mapValues(v -> v.toString() + " companies")
                .peek((s, s2) -> System.out.println(s + " " + s2))
                .to("topic-count-companies-by-country", Produced.with(Serdes.String(), Serdes.String()));

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

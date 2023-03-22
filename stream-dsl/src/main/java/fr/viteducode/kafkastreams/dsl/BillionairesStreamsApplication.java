package fr.viteducode.kafkastreams.dsl;

import fr.viteducode.kafkaclient.producer.simple.data.BillionaireKey;
import fr.viteducode.kafkaclient.producer.simple.data.BillionaireValue;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class BillionairesStreamsApplication {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kstream-billionaires-store");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-parsing-billionaire");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        properties.put("auto.offset.reset", "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<BillionaireKey, BillionaireValue> stream = builder.stream("topic-billionaires");

        stream
                .filter((BillionaireKey, BillionaireValue) -> "France".equals(BillionaireValue.getCountry()) || "United States".equals(BillionaireValue.getCountry()))
                .peek((BillionaireKey, BillionaireValue) -> System.out.println(BillionaireValue.getCountry()))
                .split()
                .branch(
                        (BillionaireKey, BillionaireValue) -> ("France".equals(BillionaireValue.getCountry())),
                        Branched.withConsumer(k -> k.to("topic-france-billionaire")))
                .branch(
                        (BillionaireKey, BillionaireValue) -> ("United States".equals(BillionaireValue.getCountry())),
                        Branched.withConsumer(k -> k.to("topic-united_states-billionaire")));


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

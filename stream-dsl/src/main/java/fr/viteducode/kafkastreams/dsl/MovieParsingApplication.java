package fr.viteducode.kafkastreams.dsl;

import fr.viteducode.avro.CastTitleKey;
import fr.viteducode.avro.CastTitleValue;
import fr.viteducode.avro.NetflixContentKey;
import fr.viteducode.avro.NetflixContentValue;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class MovieParsingApplication {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "parsing-content-movie-app");
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://0.0.0.0:8081");
        properties.put("auto.offset.reset", "earliest");

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://0.0.0.0:8081");

        final Serde<NetflixContentKey> incomingSerdeAvroKey = new SpecificAvroSerde<>();
        incomingSerdeAvroKey.configure(serdeConfig, true);

        final Serde<NetflixContentValue> incomingSerdeAvroValue = new SpecificAvroSerde<>();
        incomingSerdeAvroValue.configure(serdeConfig, false);

        final Serde<CastTitleKey> outcomingSerdeAvroKey = new SpecificAvroSerde<>();
        outcomingSerdeAvroKey.configure(serdeConfig, true);

        final Serde<CastTitleValue> outcomingSerdeAvroValue = new SpecificAvroSerde<>();
        outcomingSerdeAvroValue.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<NetflixContentKey, NetflixContentValue> stream = builder.stream("topic-movie-content", Consumed.with(incomingSerdeAvroKey, incomingSerdeAvroValue));

        stream
                .filter((key, value) -> !value.getCast().isEmpty())
                .flatMap((key, value) -> Arrays.stream(value.getCast().split(",")).map(cast -> {

                    String trimmedCast = cast.trim();

                    CastTitleKey castTitleKey = CastTitleKey.newBuilder().setName(trimmedCast).build();

                    CastTitleValue castTitleValue = CastTitleValue.newBuilder()
                            .setTitle(value.getTitle())
                            .setName(trimmedCast)
                            .setReleaseYear(value.getReleaseYear())
                            .setShowId(value.getShowId())
                            .build();

                    return KeyValue.pair(castTitleKey, castTitleValue);

                }).collect(Collectors.toList()))
                .to("topic-movie-cast", Produced.with(outcomingSerdeAvroKey, outcomingSerdeAvroValue));

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

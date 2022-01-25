package fr.viteducode.kafkastreams.dsl;

import fr.viteducode.avro.*;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class MovieParsingApplication {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "./kstream-store");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "parsing-content-movie-app");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://0.0.0.0:8081");
        properties.put("auto.offset.reset", "earliest");

        /*final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://0.0.0.0:8081");

        final Serde<NetflixContentKey> incomingSerdeAvroKey = new SpecificAvroSerde<>();
        incomingSerdeAvroKey.configure(serdeConfig, true);

        final Serde<NetflixContentValue> incomingSerdeAvroValue = new SpecificAvroSerde<>();
        incomingSerdeAvroValue.configure(serdeConfig, false);

        final Serde<CastTitleKey> outcomingSerdeAvroKey = new SpecificAvroSerde<>();
        outcomingSerdeAvroKey.configure(serdeConfig, true);

        final Serde<CastTitleValue> outcomingSerdeAvroValue = new SpecificAvroSerde<>();
        outcomingSerdeAvroValue.configure(serdeConfig, false);*/

        StreamsBuilder builder = new StreamsBuilder();

        KStream<NetflixContentKey, NetflixContentValue> stream = builder.stream("topic-movie-content");

        stream
                .filter((key, value) -> !value.getCast().isEmpty())
                .flatMap((key, value) -> Arrays.stream(value.getCast().split(",")).map(cast -> {

                    String trimmedCast = cast.trim();

                    ActorTitleKey castTitleKey = ActorTitleKey.newBuilder().setActorName(trimmedCast).build();

                    ActorTitleValue castTitleValue = ActorTitleValue.newBuilder()
                            .setTitle(value.getTitle())
                            .setActorName(trimmedCast)
                            .setReleaseYear(value.getReleaseYear())
                            .setShowId(value.getShowId())
                            .build();

                    return KeyValue.pair(castTitleKey, castTitleValue);

                }).collect(Collectors.toList()))
                .through("topic-actor-movie")
                .groupByKey()
                .aggregate(
                        () -> ActorWithTitlesValue.newBuilder().setTitles(new ArrayList<>()).build(),
                        (actorTitleKey, actorTitleValue, actorWithTitlesValue) -> {
                            actorWithTitlesValue.getTitles().add(actorTitleValue.getTitle());
                            return actorWithTitlesValue;
                        }
                        , Materialized.as("topic-actor-with-titles-movie-store"))
                .toStream().to("topic-actor-with-titles-movie");


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

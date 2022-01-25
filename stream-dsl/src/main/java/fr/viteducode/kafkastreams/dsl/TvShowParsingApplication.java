package fr.viteducode.kafkastreams.dsl;

import fr.viteducode.avro.*;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class TvShowParsingApplication {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "parsing-content-tvshow-app");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://0.0.0.0:8081");
        properties.put("auto.offset.reset", "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<NetflixContentKey, NetflixContentValue> stream = builder.stream("topic-tvshow-content");

        stream
                .filterNot((key, value) -> value.getCast().isEmpty())
                .flatMapValues(value -> Arrays.stream(value.getCast().split(",")).map(cast -> {

                    String trimmedCast = cast.trim();

                    return ActorTitleValue.newBuilder()
                            .setTitle(value.getTitle())
                            .setActorName(trimmedCast)
                            .setReleaseYear(value.getReleaseYear())
                            .setShowId(value.getShowId())
                            .build();

                }).collect(Collectors.toList()))
                .selectKey((titleKey, value) -> ActorTitleKey.newBuilder().setActorName(value.getActorName()).build())
                .through("topic-actor-tvshow")
                .groupBy((actorTitleKey, actorTitleValue) -> actorTitleKey)
                .aggregate(
                        () -> ActorWithTitlesValue.newBuilder().setTitles(new ArrayList<>()).build(),
                        (actorTitleKey, actorTitleValue, actorWithTitlesValue) -> {
                            actorWithTitlesValue.getTitles().add(actorTitleValue.getTitle());
                            return actorWithTitlesValue;
                        }
                        , Materialized.as("topic-actor-with-titles-tvshow-store"))
                .toStream()
                .to("topic-actor-with-titles-tvshow");

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

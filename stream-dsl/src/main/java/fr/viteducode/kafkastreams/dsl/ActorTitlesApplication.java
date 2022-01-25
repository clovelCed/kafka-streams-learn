package fr.viteducode.kafkastreams.dsl;

import fr.viteducode.avro.ActorTitleKey;
import fr.viteducode.avro.ActorWithAllTitlesValue;
import fr.viteducode.avro.ActorWithTitlesValue;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ActorTitlesApplication {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "./kstream-store");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-parsing-actor-titles");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://0.0.0.0:8081");
        properties.put("auto.offset.reset", "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KTable<ActorTitleKey, ActorWithTitlesValue> streamMovie = builder.table("topic-actor-with-titles-movie");
        KTable<ActorTitleKey, ActorWithTitlesValue> streamTvShow = builder.table("topic-actor-with-titles-tvshow");

        streamMovie.join(streamTvShow, (actorTitleValueMovie, actorTitleValueTvShow) -> {

            return ActorWithAllTitlesValue
                    .newBuilder()
                    .setTitlesMovie(actorTitleValueMovie.getTitles())
                    .setTitlesTvShow(actorTitleValueTvShow.getTitles())
                    .build();
        })
                .toStream()
                .filter(
                        (actorTitleKey, actorWithAllTitlesValue) -> !actorWithAllTitlesValue.getTitlesMovie().isEmpty() && !actorWithAllTitlesValue.getTitlesTvShow().isEmpty()
                )
                .to("topic-actor-with-all-titles");


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

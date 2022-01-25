package fr.viteducode.kafkastreams.dsl;

import fr.viteducode.avro.NetflixContentKey;
import fr.viteducode.avro.NetflixContentValue;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ParsingContentApplication {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "parsing-content-app");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://0.0.0.0:8081");
        properties.put("auto.offset.reset", "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stream = builder.stream("topic-netflix-content", Consumed.with(Serdes.String(), Serdes.String()));

        KStream<NetflixContentKey, NetflixContentValue> fullTitleStream = stream.map((key, value) -> {

            String[] values = value.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            NetflixContentKey contentKey = NetflixContentKey.newBuilder().setShowId(values[0]).build();

            NetflixContentValue contentValue = NetflixContentValue.newBuilder()
                    .setShowId(values[0])
                    .setType(values[1])
                    .setTitle(values[2])
                    .setDirector(values[3])
                    .setCast(values[4].replaceAll("\"", "").trim())
                    .setCountry(values[5])
                    .setDateAdded(values[6].replaceAll("\"", "").trim())
                    .setReleaseYear(values[7])
                    .setRating(values[8])
                    .setDuration(values[9])
                    .build();

            return KeyValue.pair(contentKey, contentValue);
        });

        KStream<NetflixContentKey, NetflixContentValue>[] branches = fullTitleStream.branch(
                (titleKey, titleValue) -> ("Movie".equals(titleValue.getType())),
                (titleKey, titleValue) -> ("TV Show".equals(titleValue.getType()))
        );

        KStream<NetflixContentKey, NetflixContentValue> movieStream = branches[0];

        KStream<NetflixContentKey, NetflixContentValue> tvShowStream = branches[1];

        movieStream.to("topic-movie-content");

        tvShowStream.to("topic-tvshow-content");

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

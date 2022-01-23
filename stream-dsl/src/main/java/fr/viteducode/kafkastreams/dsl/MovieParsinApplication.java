package fr.viteducode.kafkastreams.dsl;

import fr.viteducode.avro.NetflixContent;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
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

public class MovieParsinApplication {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-app-1");
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://0.0.0.0:8081");
        properties.put("auto.offset.reset", "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stream = builder.stream("topic-source", Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, NetflixContent> fullTitleStream = stream.map((key, value) -> {

            String[] values = value.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            KeyValue<String, NetflixContent> pair = null;

            try {
                NetflixContent content = NetflixContent.newBuilder()
                        .setShowId(values[0])
                        .setType(values[1])
                        .setTitle(values[2])
                        .setDirector(values[3])
                        .setCast(values[4].replaceAll("\"", ""))
                        .setCountry(values[5])
                        .setDateAdded(values[6].replaceAll("\"", ""))
                        .setReleaseYear(values[7])
                        .setRating(values[8])
                        .setDuration(values[9])
                        .build();

                pair = KeyValue.pair(content.getShowId(), content);
            } catch (ArrayIndexOutOfBoundsException e) {
                System.out.println("args = [" + args + "]");
            }


            return pair;
        });

        KStream<String, NetflixContent>[] branches = fullTitleStream.branch(
                (titleKey, titleValue) -> ("Movie".equals(titleValue.getType())),
                (titleKey, titleValue) -> ("TV Show".equals(titleValue.getType()))
        );

        KStream<String, NetflixContent> movieStream = branches[0];

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://0.0.0.0:8081");
        final Serde<NetflixContent> serdeAvroValue = new SpecificAvroSerde<>();
        serdeAvroValue.configure(serdeConfig, false);

        movieStream
                .filter((key, value) -> !value.getCast().isEmpty())
                .flatMap((key, value) -> Arrays.stream(value.getCast().split(",")).map(cast -> KeyValue.pair(cast, value)).collect(Collectors.toList()))
                //.peek((titleKey, value) -> System.out.println(value))
                .to("topic-movie-cast", Produced.with(Serdes.String(), serdeAvroValue));

        KStream<String, NetflixContent> tvShowStream = branches[1];
        tvShowStream
                .filterNot((key, value) -> value.getCast().isEmpty())
                .flatMapValues(value -> Arrays.stream(value.getCast().split(",")).collect(Collectors.toList()))
                .selectKey((titleKey, value) -> value)
                .peek((titleKey, value) -> {
                    System.out.println("args = [" + args + "]");
                    System.out.printf("Show -> KEY : %s - VALUE : %s", titleKey, value);
                });
        //.to("topic-show-cast", Produced.with(Serdes.String(), Serdes.String()));

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

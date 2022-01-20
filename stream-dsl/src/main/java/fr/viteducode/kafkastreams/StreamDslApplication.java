package fr.viteducode.kafkastreams;

import fr.viteducode.avro.TitleKey;
import fr.viteducode.avro.TitleValue;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamDslApplication {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-app-1");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stream = builder.stream("topic-source", Consumed.with(Serdes.String(), Serdes.String()));

        KStream<TitleKey, TitleValue> fullTitleStream = stream.map((key, value) -> {

            String[] values = value.split(",");

            TitleKey titleKey = TitleKey.newBuilder()
                    .setShowId(values[0])
                    .build();

            TitleValue titleValue = TitleValue.newBuilder()
                    .setType(values[1])
                    .setTitle(values[2])
                    .setDirector(values[3])
                    .setCast(values[4])
                    .setCountry(values[5])
                    .setDateAdded(values[6])
                    .setReleaseYear(Integer.parseInt(values[7]))
                    .setRating(values[8])
                    .setDuration(values[9])
                    .build();

            return KeyValue.pair(titleKey, titleValue);
        });

        KStream<TitleKey, TitleValue> titleGreaterThan2000 = fullTitleStream.filter((titleKey, titleValue) -> titleValue.getReleaseYear() > 2000);

        KStream<TitleKey, TitleValue>[] branches = titleGreaterThan2000.branch(
                (titleKey, titleValue) -> ("Movie".equals(titleValue.getType())),
                (titleKey, titleValue) -> ("TV Show".equals(titleValue.getType()))
        );

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

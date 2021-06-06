package com.annakhuseinova;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCount {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        /**
         * Enabling exactly once semantic
         * */
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        /**
         * 1. Create a stream from Kafka topic
         * */
        KStream<String, String> wordCountInput = streamsBuilder.stream("word-count-input");

        /**
         * 2. Map values to lowercase
         * */
        KTable<String, Long> wordCounts =  wordCountInput.mapValues(value -> value.toLowerCase())
         /**
          * 3. Flatmap values split by space
          * */
        .flatMapValues(value -> Arrays.asList(value.split("")))
         /**
          * 4. Select key to apply a key (we discard the old key). selectKey() takes 2 parameters - the key and
          * the value and returns the key of the record. Basically, it forms the key, to be short
          * */
        .selectKey((key, word) -> word)
         /**
          * 5. Group by key before aggregation
          * */
        .groupByKey()
        /**
         * 6. Count occurrences
         * */
        .count();

        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        /**
         * A Kafka client that allows for performing continuous computation on input coming from one or
         * more input topics and sends output to zero, one, or more output topics.
         * */
        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), config);
        streams.start();

        /**
         * Printing the topology
         * */
        System.out.println(streams.toString());

        /**
         * Shutdown hook to correctly close the streams application
         * */
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}

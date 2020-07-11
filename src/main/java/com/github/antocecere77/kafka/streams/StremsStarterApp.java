package com.github.antocecere77.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Arrays;
import java.util.Properties;

public class StremsStarterApp {

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();
        //1. Streams from Kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input");
        //2. Map to lowercase
        KTable<String, Long> wordCounts = wordCountInput.mapValues(textLine -> textLine.toLowerCase())
                //Can be alternatively writteen as
                //.mapValues(String:toLowerCase)
                //3. Flatmap values split by space
                .flatMapValues(lowerCaseTextLine -> Arrays.asList(lowerCaseTextLine.split(" ")))
                //4. Select key to apply a key (we discard the old key)
                .selectKey((ignoredKey, word) -> word)
                //5. Group by key before aggregation
                .groupByKey()
                //6. Count occurences
                .count("Counts");
        //7. to in order to write the results back to Kafka
        wordCounts.to(Serdes.String(), Serdes.Long(), "word-count-output");

        TopologyBuilder builder1;
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        //Printed the topology
        System.out.println(streams.toString());

        //Shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}

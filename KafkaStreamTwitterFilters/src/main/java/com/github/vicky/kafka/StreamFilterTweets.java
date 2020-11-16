package com.github.vicky.kafka;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamFilterTweets {

    private static JsonParser jsonParser = new JsonParser();

    private static Integer extractUserFollowersInTweets(String tweetJSON){
        try{
            return jsonParser.parse(tweetJSON)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        }catch (NullPointerException e){
            return 0;
        }
    }

    public static void main(String[] args) {
        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"my-kafka-stream");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter-tweets");
        KStream<String, String> filteredStream = inputTopic.filter(
                //filter for tweets with user>10000 followers
                (k, jsonTweets) -> extractUserFollowersInTweets(jsonTweets) > 1000
        );
        filteredStream.to("important_tweets");

        //build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(),
                properties
        );

        //start the streaming application
        kafkaStreams.start();
    }
}

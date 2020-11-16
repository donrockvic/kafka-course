package com.github.vicky.ElasticSearch.tutorial3;


import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.transform.sax.SAXResult;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;


public class ElasticSearchConsumer {

    /**
     * Class for RESTAPI for connecting and sending data to Online Elasticssearch
     * @return RestHighLevelClient
     */
    public static RestHighLevelClient createClient(){
        //Replce with new credential
        String hostname = "udemy-4322159112.us-east-1.bonsaisearch.net";
        String username = "1o1ftu0fti";
        String password = "obahzqykun";

        //
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https")).
                setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    /**
     * Consumer class
     * @return
     */
    public static KafkaConsumer<String, String> createConsumer(String topic){
        String bootstrapServer = "127.0.0.1:9092";
        String group_id = "kafka-ElasticSearch";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earliest/None/Latest
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false"); //disable auto commit of offset
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"20");

        //Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return  consumer;
     }

//     private static JsonParser jsonParser =  new JsonParser();
//     private static String exractIdFromJSON(String jsonData){
//        String id = jsonParser.parse(jsonData).getAsJsonObject().get("id_Stf").getAsString();
//        return id;
//     }


    /**
     * Main Class
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        String topic = "twitter-tweets";

        Logger logger =  LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer(topic);
        while(true){
            ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100));
            logger.info("Received :"+records.count()+" records.");

            BulkRequest bulkRequest =  new BulkRequest();

            for(ConsumerRecord<String, String> record : records){
                try{
                    String id =  record.topic()+"_"+record.partition()+"_"+record.offset();
                    //String id = exractIdFromJSON(record.value());
                    //Here we will insert data into ElasticSearch
                    String jsonString = record.value();

                    IndexRequest request = new IndexRequest("twitter");
                    request.id(id); //make sure our consumer idoempotenet
                    request.source(jsonString, XContentType.JSON);

                    bulkRequest.add(request);
                }catch (NullPointerException e){
                    logger.error("Skipping bad data : "+record.value());
                }
            }
            if(records.count()>0){
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("committing sync...");
                consumer.commitSync();
                logger.info("offset has been committed");
            }
        }

        // client.close();
    }
}

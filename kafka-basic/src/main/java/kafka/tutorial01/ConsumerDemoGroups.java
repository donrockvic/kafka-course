package kafka.tutorial01;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoGroups {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        String bootstrapServer = "127.0.0.1:9092";
        String group_id = "my_app04";
        String topic = "app00";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer .class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earliest/None/Latest


        //Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe consumer to topic(s)
        //consumer.subscribe(Arrays.asList(topic1, topic2, topic3));
        consumer.subscribe(Collections.singleton(topic));

        // poll for new data
        while(true){
            ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record : records){
                logger.info("key: "+record.key() + " | Value: "+record.value());
                logger.info("Partitions: "+ record.partition() + " | Offsets: "+ record.offset());
            }
        }
    }
}

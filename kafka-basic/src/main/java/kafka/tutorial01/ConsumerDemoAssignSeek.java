package kafka.tutorial01;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

        String bootstrapServer = "127.0.0.1:9092";
        String topic = "app00";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earliest/None/Latest


        //Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Assign and Seek are mostly used ti replay data and Fetch Specific messages
        TopicPartition partitionToReadFrom = new TopicPartition(topic,0);
        long OffsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //Seek
        consumer.seek(partitionToReadFrom, OffsetToReadFrom );

        int NoMessagesRead = 5;
        boolean KeepReading = true;
        int currentMessge = 0;
        // poll for new data
        while (KeepReading){
            currentMessge++;
            ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record : records){
                logger.info("key: "+record.key() + " | Value: "+record.value());
                logger.info("Partitions: "+ record.partition() + " | Offsets: "+ record.offset());
                if(currentMessge>=NoMessagesRead){
                    KeepReading= false;
                    break;
                }
            }

        }
        logger.info("Exiting the application");
    }
}

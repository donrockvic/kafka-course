package kafka.tutorial01;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

        String bootstrapServer = "127.0.0.1:9092";

        // Create Poducer preperties
        Properties properties = new Properties();
        // Hard Code way
        // properties.setProperty("bootstrap.servers",bootstrapServer);
        // properties.setProperty("key.serializer", StringSerializer.class.getName());
        // properties.setProperty("value.serializer",StringSerializer.class.getName());
        // New Way
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int i=0; i < 10; i++){
            // Create producer Record
            ProducerRecord<String, String> record = new ProducerRecord<>("app00", "Hello has the new World");

            //send Data -- asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //execute everytime a record is sent sucessfully or an exception is thrown
                    if(e == null){
                        // the record was sent sucessfully
                        logger.info("Recived the Metadata .\n"+
                                "Topic: "+recordMetadata.topic()+"\n"+
                                "Partition: "+recordMetadata.partition()+"\n"+
                                "Offset: "+recordMetadata.offset()+"\n"+
                                "TimeStamp: "+recordMetadata.timestamp()
                                );

                    }else{
                        logger.error("Error While Producing : ", e);
                    }
                }
            });
        }

        producer.flush();
        producer.close();

     }
}

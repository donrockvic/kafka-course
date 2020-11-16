package kafka.tutorial01;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    private ConsumerDemoWithThreads(){
    }

    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        //Latch for dealing with multile thread
        CountDownLatch latch = new CountDownLatch(1);

        String bootstrapServer = "127.0.0.1:9092";
        String group_id = "my_app05";
        String topic = "twitter-tweets";

        // Create the consumer Runnable
        logger.info("Creating the Consumer Thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(latch,bootstrapServer,group_id,topic);

        // Start the Thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            logger.info("Caught Shutdown Hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            }catch (InterruptedException e){
                logger.error("Application got interuupted ", e);
            }finally {
                logger.info("Application Exited");
            }
        }));

        try {
            latch.await();
        }catch (InterruptedException e){
            logger.error("Application got interuupted ", e);
        }finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable{

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);


        public ConsumerRunnable(CountDownLatch latch,String bootstrapServer, String group_id, String topic){
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer .class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_id);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earliest/None/Latest

            consumer = new KafkaConsumer<>(properties);

            // Subscribe consumer to topic(s)
            //consumer.subscribe(Arrays.asList(topic1, topic2, topic3));
            consumer.subscribe(Collections.singleton(topic));

        }

        @Override
        public void run() {
            try {
                // poll for new data
                while(true){
                    ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100));

                    for(ConsumerRecord<String, String> record : records){
                        logger.info("key: "+record.key() + " | Value: "+record.value());
                        logger.info("Partitions: "+ record.partition() + " | Offsets: "+ record.offset());
                    }
                }
            }catch (WakeupException e){
                logger.info("Received Shutdown signal!");
            }finally {
                consumer.close();
                // tell our main code we are done with the consumer
                latch.countDown();
            }

        }

        public  void  shutdown(){
            // wakeup() is special method to interrupt consumer.poll()
            //it will throw the Exception WakeUpException
            consumer.wakeup();
        }
    }

}

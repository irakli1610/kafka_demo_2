package org.learning.producerandconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerDemoWithCooperative {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithCooperative.class.getSimpleName());

    public static void main(String[] args){
        log.info("kafka consumer");


        String groupId = "second-java-application";
        String topic = "demo_java";

        //create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        //consumer Properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer",StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());


        //create kafka consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        //get reference to the main thread
        final Thread mainThread = Thread.currentThread();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
               log.info("Detecting shutdown, exiting by calling consumer.wakeup()");
               consumer.wakeup();

               // join the main thread to allow the execution of the code in the main thread
                try{
                    mainThread.join();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });

        try{
            //subscribe to topic
            consumer.subscribe(Arrays.asList(topic));

            while(true) {
                log.info("pulling");

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));


                for (ConsumerRecord<String, String> record : records){
                    log.info("Key: "+ record.key()+ " value: "+ record.value());
                    log.info("Partition: "+ record.partition()+  " offset: "+ record.offset());

                }
            }
        }catch (WakeupException e){
            log.info("consumer is shutting down");

        }catch (Exception e){
            log.error("unexpected exception", e);
        }finally {
            consumer.close();
            log.info("consumer is shut down gracefully");
        }


    }
}

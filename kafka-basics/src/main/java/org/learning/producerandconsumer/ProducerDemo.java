package org.learning.producerandconsumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args){
        log.info("kafka producer");


        //create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        //producer Properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());


        // create the Producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        //create a producer record
        ProducerRecord<String,String> producerRecord =
                new ProducerRecord<>("demo_java","third message");

        //send data
        producer.send(producerRecord);

        //flush and close the producer
        producer.flush();
        producer.close();// when you call this method it automatically calls flush method too.
    }
}

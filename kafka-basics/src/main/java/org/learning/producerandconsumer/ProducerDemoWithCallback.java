package org.learning.producerandconsumer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

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



        for (int i=0; i<10; i++){
            //create a producer record
            ProducerRecord<String,String> producerRecord =
                    new ProducerRecord<>("demo_java","message from callback"+i);
            //send data
            producer.send(producerRecord, new Callback()   {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes when record is sent successfully or exception is thrown
                    if(e==null){
                        log.info("Received new metadata \n"+
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp() + "\n");
                    }else {
                        log.error("Error while producing", e);
                    }
                }
            });
        }




        //flush and close the producer
        producer.flush();
        producer.close();// when you call this method it automatically calls flush method too.
    }
}

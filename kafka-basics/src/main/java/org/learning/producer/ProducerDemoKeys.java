package org.learning.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("kafka producer");


        //create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //producer Properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());


        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int j=0; j<2;j++){
            for (int i = 0; i < 30; i++) {

                String topic = "demo_java";
                String key = "id_"+i;
                String value = "hello world "+ i;


                //create a producer record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic,key,value);
                //send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //executes when record is sent successfully or exception is thrown
                        if (e == null) {
                            log.info("Key: "+ key + " ||Partition: " + recordMetadata.partition() );
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }




        //flush and close the producer
        producer.flush();
        producer.close();// when you call this method it automatically calls flush method too.
    }
}

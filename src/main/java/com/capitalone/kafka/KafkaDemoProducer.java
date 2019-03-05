package com.capitalone.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by krf685 on 7/20/16.
 */
public class KafkaDemoProducer {

    private Producer producer;

    private KafkaProducer kafkaProducer;

    private String topicName = "kafka-demo";

    private void initialize(){
        Properties properties = new Properties();
        //Assign localhost id
        properties.put("bootstrap.servers","localhost:9092");

//        The number of acknowledgments the producer requires the leader to have received before considering a request complete. Mainly for message durablility
//        acks=0 If set to zero then the producer will not wait for any acknowledgment from the server at all.
//        acks=all This means the leader will wait for the full set of in-sync replicas to acknowledge the record.
        properties.put("acks","all");

                //If the request fails, the producer can automatically retry,
        properties.put("retries", 0);

        //Specify buffer size in config
        //The producer will attempt to batch records together into fewer requests whenever multiple records are being sent to the same partition.
        properties.put("batch.size", 16384);

        //Reduce the no of requests less than 0
//        The producer groups together any records that arrive in between request transmissions into a single batched request.
//        Normally this occurs only under load when records arrive faster than they can be sent out.
//        However in some circumstances the client may want to reduce the number of requests even under moderate load.
        properties.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        //The total bytes of memory the producer can use to buffer records waiting to be sent to the server.
        properties.put("buffer.memory", 33554432);

        //Serializer class for key that implements the Serializer interface.
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        //Serializer class for value that implements the Serializer interface.
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        kafkaProducer = new KafkaProducer<String, String>(properties);
    }

    private void sendMessages(){
        for (int i = 0; i < 50; i++) {
            kafkaProducer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i)));
            System.out.println("Message Send Successfully " + i);
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
//            producer.close();
        }
    }

    public static void main(String[] args) {
        KafkaDemoProducer demoProducer = new KafkaDemoProducer();
        demoProducer.initialize();
        demoProducer.sendMessages();
        if(demoProducer.producer != null) demoProducer.producer.close();
    }
}

package com.capitalone.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by krf685 on 7/20/16.
 */
public class KafkaDemoConsumer {

    private ConsumerConnector consumerConnector = null;

    private String topic = "uber-topic";

    public void initialize(){
        Properties props = new Properties();
//        props.put("zookeeper.connect", "internal-COAF-LS-KAFKA-QA-CLUSTER-1848818175.us-east-1.elb.amazonaws.com:11132");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "test");

        //Zookeeper session timeout
        props.put("zookeeper.session.timeout.ms", "6000");

//How far a ZK follower can be behind a ZK leader
        props.put("zookeeper.sync.time.ms", "300");

//        The frequency in ms that the consumer offsets are committed to zookeeper.
        props.put("auto.commit.interval.ms", "1000");
        ConsumerConfig conConfig = new ConsumerConfig(props);
        consumerConnector = Consumer.createJavaConsumerConnector(conConfig);
    }

    public void consume() throws Exception {
        //Key = topic name, Value = No. of threads for topic
        Map<String, Integer> topicCount = new HashMap<String, Integer>();
        topicCount.put(topic, new Integer(1));

        //ConsumerConnector creates the message stream for each topic
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams =
                consumerConnector.createMessageStreams(topicCount);

        // Get Kafka stream for topic 'kafka-demo-2'
        List<KafkaStream<byte[], byte[]>> kStreamList =
                consumerStreams.get(topic);
        // Iterate stream using ConsumerIterator
        int count = 0;
        for (KafkaStream<byte[], byte[]> kStreams : kStreamList) {
            ConsumerIterator<byte[], byte[]> consumerIte = kStreams.iterator();

            while (consumerIte.hasNext()) {

                String message = new String(consumerIte.next().message());
                count += count;
                System.out.println("message = " + message);
            }
            System.out.println("Total Records Processed = " + count);
        }
        //Shutdown the consumer connector
        if (consumerConnector != null)   consumerConnector.shutdown();
    }

    public static void main(String[] args) {
        KafkaDemoConsumer demoConsumer = new KafkaDemoConsumer();
        demoConsumer.initialize();
        try {
            demoConsumer.consume();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

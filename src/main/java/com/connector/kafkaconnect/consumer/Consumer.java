package com.connector.kafkaconnect.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
public class Consumer {

    public void processMessage(ConsumerRecord<String, String> data) {
        System.out.println("data = " + data);
    }
}

package com.connector.kafkaconnect.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

@Component
public class DefaultProducerListener implements ProducerListener<String, String> {

    @Override
    public void onSuccess(ProducerRecord<String, String> producerRecord, RecordMetadata recordMetadata) {
        System.out.println("Sent message=[" + producerRecord + "] with offset=[" + recordMetadata.offset() + "]");
    }

    @Override
    public void onError(ProducerRecord<String, String> producerRecord, @Nullable RecordMetadata recordMetadata, Exception exception) {
        System.out.println("Unable to send message=[" + producerRecord + "] due to : " + exception.getMessage());
    }
}
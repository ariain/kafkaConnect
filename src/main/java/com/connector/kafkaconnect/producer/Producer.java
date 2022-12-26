package com.connector.kafkaconnect.producer;

import com.connector.kafkaconnect.KafkaConfiguration;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

@Service
@RequiredArgsConstructor
public class Producer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaConfiguration kafkaConfiguration;

    public SendResult<String, String> sendMessage(String msg) throws ExecutionException, InterruptedException {
        return kafkaTemplate.send(kafkaConfiguration.getProducerTopic(), msg).get();
    }
}

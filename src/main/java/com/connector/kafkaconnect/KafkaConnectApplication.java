package com.connector.kafkaconnect;

import com.connector.kafkaconnect.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
@RestController
public class KafkaConnectApplication {

    @Autowired
    private Producer producer;

    public static void main(String[] args) {
        SpringApplication.run(KafkaConnectApplication.class, args);
    }

    @PostMapping("/sendMessage")
    public SendResult<String, String> sendMessage(@RequestBody Map<String, String> body) throws ExecutionException, InterruptedException {
        return producer.sendMessage(body.get("message"));
    }
}

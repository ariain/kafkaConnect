package com.connector.kafkaconnect.configuration;

import com.connector.kafkaconnect.consumer.Consumer;
import com.connector.kafkaconnect.producer.DefaultProducerListener;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfiguration implements KafkaListenerConfigurer {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServer;

    @Value("${spring.kafka.consumer.topic}")
    private String consumerTopic;

    @Getter
    @Value("${spring.kafka.producer.topic}")
    private String producerTopic;

    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;

    @Autowired
    private BeanFactory beanFactory;

    @Autowired
    private Consumer consumer;

    @Autowired
    private DefaultProducerListener producerListener;

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return new DefaultKafkaProducerFactory<>(configProps, new StringSerializer(), new StringSerializer());
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());
        kafkaTemplate.setDefaultTopic(producerTopic);
        kafkaTemplate.setProducerListener(producerListener);
        return kafkaTemplate;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> buildConsumer() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        configMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configMap.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);

        ConcurrentKafkaListenerContainerFactory<String, String> containerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        containerFactory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(configMap));
        return containerFactory;
    }

    @SneakyThrows
    @Override
    public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
        MethodKafkaListenerEndpoint<String, String> kafkaConsumerListenerEndpoint = new MethodKafkaListenerEndpoint<>();
        kafkaConsumerListenerEndpoint.setId("test");
        kafkaConsumerListenerEndpoint.setGroupId(consumerGroupId);
        kafkaConsumerListenerEndpoint.setBeanFactory(beanFactory);
        kafkaConsumerListenerEndpoint.setBean(consumer);
        kafkaConsumerListenerEndpoint.setAutoStartup(true);
        kafkaConsumerListenerEndpoint.setTopics(consumerTopic);
        kafkaConsumerListenerEndpoint.setMethod(consumer.getClass().getDeclaredMethod("processMessage", ConsumerRecord.class));
        kafkaConsumerListenerEndpoint.setMessageHandlerMethodFactory(new DefaultMessageHandlerMethodFactory());

        registrar.registerEndpoint(kafkaConsumerListenerEndpoint);
    }
}

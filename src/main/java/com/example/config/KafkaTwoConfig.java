package com.example.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * kafka 连接配置
 */
@EnableKafka
@Configuration
public class KafkaTwoConfig {

    @Value("${spring.kafka.two.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${spring.kafka.two.consumer.group-id}")
    private String groupId;
    @Value("${spring.kafka.two.consumer.enable-auto-commit}")
    private boolean enableAutoCommit;
    @Value("${spring.kafka.two.consumer.auto-offset-reset}")
    private String autoOffsetReset;
    @Value("${spring.kafka.two.consumer.max-poll-reocrds}")
    private String  maxPollRecords;
    @Value("${spring.kafka.two.security_enabled}")
    private boolean securitEnabled;

    @Value("${spring.kafka.kerberos_path}")
    private String kerberosPath;

    @Bean
    public KafkaTemplate<String, String> kafkaTwoTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>> kafkaTwoContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        factory.getContainerProperties().setPollTimeout(3000);
        //MANUAL   当每一批poll()的数据被消费者监听器（ListenerConsumer）处理之后, 手动调用Acknowledgment.acknowledge()后提交
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        // 设置批量消费，默认单条
        factory.setBatchListener(true);
        return factory;
    }

    private ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    public ConsumerFactory<Integer, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    private Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    private Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  autoOffsetReset);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,  maxPollRecords);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);


        // 认证代码
        if(securitEnabled){
            System.setProperty("java.security.krb5.conf",kerberosPath+"krb5.conf");
            System.setProperty("java.security.auth.login.config",kerberosPath+"kafka-jaas.conf");

            props.put("sasl.kerberos.service.name", "kafka");
            props.put("sasl.mechanism", "GSSAPI");
            props.put("security.protocol", "SASL_PLAINTEXT");
        }
        return props;
    }
}

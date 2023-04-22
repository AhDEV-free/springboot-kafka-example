package com.example.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 中台kafka 数据消费
 */
@Component
public class TcKafkaComsumer {
    Logger log = LoggerFactory.getLogger(TcKafkaComsumer.class);

    @KafkaListener(topics = "${spring.kafka.two.consumer.topic.scada1}", containerFactory = "kafkaTwoContainerFactory")
    public void zfz1Consumer(List<String> messages, Acknowledgment ack) {
        log.info("[Consumer] RECV MSG COUNT: {}", messages.size());
        log.info("[Consumer] RECV MSG[0]: {}", messages.get(0));

        //确认单当前消息（及之前的消息）offset均已被消费完成
        ack.acknowledge();
    }

}

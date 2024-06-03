package com.org.service.consumer;

import com.alibaba.fastjson.JSON;
import com.org.constants.Constants;
import com.org.entity.InterKafkaMsgDTO;
import com.org.entity.KafkaMsgDTO;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class OrderConsumer {

    @Resource
    private OrderThreadPool<OrderWorkerHandler, InterKafkaMsgDTO> orderThreadPool;

    @KafkaListener(topics = Constants.TOPIC_ORDER, groupId = "orderGroup", concurrency = "3")
    public void logListener(ConsumerRecord<String, String> record) {
        log.debug("> receive log event: {}-{}", record.partition(), record.value());
        try {
            KafkaMsgDTO kafkaMsgDTO = JSON.parseObject(record.value(), KafkaMsgDTO.class);

            InterKafkaMsgDTO interKafkaRecord = new InterKafkaMsgDTO(String.valueOf(record.partition()), kafkaMsgDTO.getBizId(), kafkaMsgDTO.getData());
            orderThreadPool.dispatch(interKafkaRecord);
        } catch (Exception e) {
            log.error("# kafka log listener error: {}", record.value(), e);
        }
    }
}

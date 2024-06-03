package com.org.api;

import com.alibaba.fastjson.JSON;
import com.org.constants.Constants;
import com.org.entity.KafkaMsgDTO;
import com.org.entity.BusinessDTO;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RestController("api/")
@Slf4j
public class MsgSendApiService {

    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;


    @PostMapping(value = "send")
    public void sendMsg() {
        Random random = new Random();
        for (int i = 0; i < 40; i++) {
            CompletableFuture.runAsync(()->{
                String bizId1 = UUID.randomUUID().toString();
                String bizId2 = UUID.randomUUID().toString();

                BusinessDTO businessDTO1 = new BusinessDTO(bizId1, 1);
                BusinessDTO businessDTO2 = new BusinessDTO(bizId2, 1);

                kafkaTemplate.send(Constants.TOPIC_ORDER, bizId1, JSON.toJSONString(new KafkaMsgDTO<>(businessDTO1.getOrderId(), businessDTO1)));
                kafkaTemplate.send(Constants.TOPIC_ORDER, bizId2, JSON.toJSONString(new KafkaMsgDTO<>(businessDTO2.getOrderId(), businessDTO2)));

                businessDTO1.setStatus(2);
                businessDTO2.setStatus(2);
                kafkaTemplate.send(Constants.TOPIC_ORDER, bizId1, JSON.toJSONString(new KafkaMsgDTO<>(businessDTO1.getOrderId(), businessDTO1)));
                kafkaTemplate.send(Constants.TOPIC_ORDER, bizId2, JSON.toJSONString(new KafkaMsgDTO<>(businessDTO2.getOrderId(), businessDTO2)));

                businessDTO1.setStatus(3);
                businessDTO2.setStatus(3);
                kafkaTemplate.send(Constants.TOPIC_ORDER, bizId1, JSON.toJSONString(new KafkaMsgDTO<>(businessDTO1.getOrderId(), businessDTO1)));
                kafkaTemplate.send(Constants.TOPIC_ORDER, bizId2, JSON.toJSONString(new KafkaMsgDTO<>(businessDTO2.getOrderId(), businessDTO2)));
            });
        }
    }
}

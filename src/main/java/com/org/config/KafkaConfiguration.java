package com.org.config;

import com.org.constants.Constants;
import com.org.entity.InterKafkaMsgDTO;
import com.org.service.consumer.OrderThreadPool;
import com.org.service.consumer.OrderWorkerHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class KafkaConfiguration {

    @Bean
    public NewTopic topic(){
        return new NewTopic(Constants.TOPIC_ORDER,3,(short) 2);
    }


    /**
     * 配置顺序消费的线程池
     *
     * @return
     */
    @Bean
    public OrderThreadPool<OrderWorkerHandler, InterKafkaMsgDTO> orderThreadPool() {
        OrderThreadPool<OrderWorkerHandler, InterKafkaMsgDTO> threadPool = new OrderThreadPool<>(3, () -> new OrderWorkerHandler(100));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutdown orderThreadPool");
            //容器关闭时让工作线程中的任务都被消费完
            threadPool.shutdown();
        }));
        return threadPool;
    }
}

package com.org.service.consumer;

import com.alibaba.fastjson.JSON;
import com.org.entity.BusinessDTO;
import com.org.entity.InterKafkaMsgDTO;
import lombok.extern.slf4j.Slf4j;

/**
 * @Date: 2024/1/24 13:42
 * 具体消费者
 */
@Slf4j
public class OrderWorkerHandler extends SingleThreadWorker<InterKafkaMsgDTO> {
    public OrderWorkerHandler(int size) {
        super(size);
    }

    @Override
    protected void doConsumer(InterKafkaMsgDTO data) {
        BusinessDTO businessDTO = JSON.parseObject(JSON.toJSONString(data.getData()), BusinessDTO.class);
        log.info("线程{} 消费消息成功,业务Id为:{},状态为:{}", Thread.currentThread().getName(), businessDTO.getOrderId(), getStatus(businessDTO.getStatus()));
    }

    private String getStatus(int status) {
        if(status == 1) return  "创建订单";
        return status == 2 ? "已支付" : "支付成功";
    }
}

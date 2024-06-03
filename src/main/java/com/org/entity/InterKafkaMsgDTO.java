package com.org.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class InterKafkaMsgDTO<T> implements OrderMessage {

    /**
     * 属于哪个分区
     */
    private String partition;

    /**
     * 主键Id
     */
    private String bizId;

    /**
     * 业务数据
     */
    private T data;

    @Override
    public String getUniqueNo() {
        return this.getBizId();
    }
}

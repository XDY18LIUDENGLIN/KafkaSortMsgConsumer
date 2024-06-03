package com.org.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class KafkaMsgDTO<T> {

    private String bizId;

    private T data;
}

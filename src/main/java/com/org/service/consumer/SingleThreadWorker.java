package com.org.service.consumer;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public abstract class SingleThreadWorker<T> implements Runnable {

    private static AtomicInteger cnt = new AtomicInteger(0);
    private BlockingQueue<T> queue;
    private boolean started = true;

    /**
     * worker 唯一id
     */
    @Getter
    private int queueNo;

    public SingleThreadWorker(int size) {
        this.queue = new LinkedBlockingQueue<>(size);
        this.queueNo = cnt.getAndIncrement();
        log.info("init worker {}", this.queueNo);
    }

    /**
     * 提交消息
     *
     * @param data
     */
    public void offer(T data) {
        try {
            queue.put(data);
        } catch (InterruptedException e) {
            log.info("{} offer error: {}", Thread.currentThread().getName(), JSON.toJSONString(data), e);
        }
    }

    @Override
    public void run() {
        log.info("{} worker start take ", Thread.currentThread().getName());
        while (started) {
            try {
                T data = queue.take();
                doConsumer(data);
            } catch (InterruptedException e) {
                log.error("queue take error", e);
            }
        }
    }

    /**
     * do real consume message
     *
     * @param data
     */
    protected abstract void doConsumer(T data);

    /**
     * consume rest of message in the queue when thread-pool shutdown
     */
    public void shutdown() {
        this.started = false;
        ArrayList<T> rest = new ArrayList<>();
        int i = queue.drainTo(rest);
        if (i > 0) {
            log.info("{} has rest in queue {}", Thread.currentThread().getName(), i);
            for (T t : rest) {
                doConsumer(t);
            }
        }
    }
}

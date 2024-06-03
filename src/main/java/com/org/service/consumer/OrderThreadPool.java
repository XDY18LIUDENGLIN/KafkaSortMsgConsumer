package com.org.service.consumer;

import com.org.entity.OrderMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 *
 * @param W: worker
 * @param D: message
 */
@Slf4j
public class OrderThreadPool<W extends SingleThreadWorker<D>, D extends OrderMessage> {

    /**
     * 工作线程线程
     */
    private List<W> workers;
    /**
     * 线程并发级别
     */
    private Integer concurrentSize;

    /**
     * 是否全量停止任务,留个钩子，以便后续动态扩容
     */

    public OrderThreadPool(int size, Supplier<W> provider) {
        this.concurrentSize = size;
        workers = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            workers.add(provider.get());
        }
        if (CollectionUtils.isEmpty(workers)) {
            throw new RuntimeException("worker size is 0");
        }
        log.info("线程配置完成,当前配置工作线程为:{}", size);
        start();
    }

    /**
     * route message to single thread
     *
     * @param data
     */
    public void dispatch(D data) {
        W w = getUniqueQueue(data.getUniqueNo());
        w.offer(data);
    }

    private W getUniqueQueue(String uniqueNo) {
        int queueNo = uniqueNo.hashCode() & Integer.MAX_VALUE % concurrentSize;
        for (W worker : workers) {
            if (queueNo == worker.getQueueNo()) {
                return worker;
            }
        }
        throw new RuntimeException("worker 路由失败");
    }

    /**
     * start worker, only start once
     */
    private void start() {
        for (W worker : workers) {
            new Thread(worker, "Worder-" + worker.getQueueNo()).start();
        }
    }

    /**
     * 关闭所有 workder, 等待所有任务执行完
     */
    public void shutdown() {
        for (W worker : workers) {
            worker.shutdown();
        }
    }

}


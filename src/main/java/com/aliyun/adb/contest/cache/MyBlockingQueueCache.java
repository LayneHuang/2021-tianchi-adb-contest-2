package com.aliyun.adb.contest.cache;

import com.aliyun.adb.contest.pool.WriteTask;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @Classname MyBufferCache
 * @Description
 * @Date 2021/6/3 16:38
 * @Created by FinkyS
 */
public final class MyBlockingQueueCache {

    private final BlockingQueue<WriteTask> bq = new LinkedBlockingDeque<>(2);
    private int size;
    public int maxSize;

    public WriteTask poll() {
        try {
            size--;
            maxSize = Math.max(maxSize, size);
            return bq.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void put(WriteTask task) {
        try {
            bq.put(task);
            size++;
            maxSize = Math.max(maxSize, size);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

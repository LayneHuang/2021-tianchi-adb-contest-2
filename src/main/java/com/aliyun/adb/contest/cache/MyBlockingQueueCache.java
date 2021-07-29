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
    private int inCont;
    private int outCount;
    public int maxCount;

    public WriteTask poll() {
        try {
            outCount++;
            maxCount = Math.max(maxCount, inCont - outCount);
            return bq.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void put(WriteTask task) {
        try {
            bq.put(task);
            inCont++;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

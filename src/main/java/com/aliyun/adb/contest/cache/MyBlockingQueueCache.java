package com.aliyun.adb.contest.cache;

import com.aliyun.adb.contest.pool.WriteTask;

import java.nio.ByteBuffer;
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

    public WriteTask poll()  {
        try {
            return bq.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void put(WriteTask task)  {
        try {
            bq.put(task);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

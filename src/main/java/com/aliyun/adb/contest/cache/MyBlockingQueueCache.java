package com.aliyun.adb.contest.cache;

import java.nio.MappedByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @Classname MyBufferCache
 * @Description
 * @Date 2021/6/3 16:38
 * @Created by FinkyS
 */
public final class MyBlockingQueueCache extends MyCache {

    private final BlockingQueue<MappedByteBuffer> bq = new LinkedBlockingDeque<>(2);

    @Override
    public MappedByteBuffer poll() throws InterruptedException {
        return bq.take();
    }

    @Override
    public void put(MappedByteBuffer buffer) throws InterruptedException {
        bq.put(buffer);
    }
}

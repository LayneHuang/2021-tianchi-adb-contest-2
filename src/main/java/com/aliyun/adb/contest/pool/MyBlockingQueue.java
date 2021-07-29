package com.aliyun.adb.contest.pool;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public final class MyBlockingQueue {

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

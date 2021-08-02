package com.aliyun.adb.contest.pool;

import com.aliyun.adb.contest.Constant;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public final class MyBlockingQueue {

    private final BlockingQueue<WriteTask> bq = new LinkedBlockingDeque<>(Constant.PAGE_COUNT);

    public WriteTask poll() {
        try {
            return bq.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void put(WriteTask task) {
        try {
            bq.put(task);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

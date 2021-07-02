package com.aliyun.adb.contest.pool;

import com.aliyun.adb.contest.Constant;
import com.aliyun.adb.contest.page.MyBlock;
import com.aliyun.adb.contest.page.MyTable;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WritePool {
    private final ExecutorService executor = Executors.newFixedThreadPool(Constant.THREAD_COUNT);

    private final BlockingQueue<MyBlock> bq;

    WritePool(BlockingQueue<MyBlock> bq) {
        this.bq = bq;
    }

    public void start(MyTable table) {
        for (int i = 0; i < table.blockCount; ++i) {
            executor.execute(this::handleBlock);
        }
    }

    public void handleBlock() {
        try {
            MyBlock block = bq.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void handleBuffer() {

    }
}

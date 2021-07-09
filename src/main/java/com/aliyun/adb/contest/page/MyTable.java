package com.aliyun.adb.contest.page;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MyTable {

    public Map<String, Integer> colIndexMap = new HashMap<>(2);

    public int blockCount;

    public MyBlock[] blocks;

    public AtomicInteger readCount = new AtomicInteger(0);

    public AtomicInteger writeCount = new AtomicInteger(0);

    public AtomicInteger pageCount = new AtomicInteger(0);

    public MyTable(int blockCount) {
        this.blockCount = blockCount;
        blocks = new MyBlock[blockCount];
    }

    public void addReadCount() {
        readCount.incrementAndGet();
    }

    public void addWriteCount() {
        writeCount.incrementAndGet();
    }

    public void addPageCount() {
        pageCount.incrementAndGet();
    }

    public boolean finished() {
        return readCount.get() == blockCount && writeCount.get() == pageCount.get();
    }
}

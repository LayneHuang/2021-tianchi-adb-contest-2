package com.aliyun.adb.contest.page;

import java.util.concurrent.atomic.AtomicInteger;

public class MyTable {
    public int blockCount;

    public LineType lineType;

    public MyBlock[] blocks;

    public AtomicInteger readCount = new AtomicInteger(0);

    public AtomicInteger writeCount = new AtomicInteger(0);

    public AtomicInteger pageCount = new AtomicInteger(0);

    public MyTable(int blockCount) {
        blocks = new MyBlock[blockCount];
    }

    public void addReadCount() {
        readCount.incrementAndGet();
    }

    public void addWriteCount() {
        writeCount.incrementAndGet();
    }

    public void addPageCount() {
        writeCount.incrementAndGet();
    }

    public boolean finished() {
        return readCount.get() == blockCount && writeCount.get() == pageCount.get();
    }
}

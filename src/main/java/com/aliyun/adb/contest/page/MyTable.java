package com.aliyun.adb.contest.page;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MyTable {

    public Map<String, Integer> colIndexMap = new HashMap<>(2);

    public int index;

    public int blockCount;

    public MyBlock[] blocks;

    public int[][][] pageCounts;

    public int dataCount;

    public AtomicInteger readCount = new AtomicInteger(0);

    public AtomicInteger writeCount = new AtomicInteger(0);

    public AtomicInteger allPageCount = new AtomicInteger(0);

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

    public void addAllPageCount() {
        allPageCount.incrementAndGet();
    }

    public boolean readFinished() {
        return readCount.get() == blockCount;
    }

    public boolean finished() {
        return readFinished() && writeCount.get() == allPageCount.get();
    }
}

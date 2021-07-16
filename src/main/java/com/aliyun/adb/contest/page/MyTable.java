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

    public boolean readFinish = false;

    public MyTable(int blockCount) {
        this.blockCount = blockCount;
        blocks = new MyBlock[blockCount];
    }

    public boolean finished() {
        return readFinish && writeCount.get() >= allPageCount.get();
    }
}

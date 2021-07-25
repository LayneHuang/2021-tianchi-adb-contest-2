package com.aliyun.adb.contest.page;

import com.aliyun.adb.contest.Constant;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MyTable {

    public Map<String, Integer> colIndexMap = new HashMap<>(2);

    public Path path;

    public int index;

    public int blockCount;

    public MyBlock[] blocks;

    public int[][][] pageCounts = new int[Constant.THREAD_COUNT][Constant.PAGE_COUNT][2];

    public int dataCount;

    public AtomicInteger readCount = new AtomicInteger(0);

    public synchronized void initBlocks(int blockCount) {
        if (this.blockCount > 0) return;
        this.blockCount = blockCount;
        blocks = new MyBlock[blockCount];
        for (int i = 0; i < blockCount; ++i) {
            blocks[i] = new MyBlock();
        }
    }
}

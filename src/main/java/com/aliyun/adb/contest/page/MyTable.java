package com.aliyun.adb.contest.page;

import com.aliyun.adb.contest.Constant;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MyTable {

    public Map<String, Integer> colIndexMap = new HashMap<>(Constant.MAX_COL_COUNT);

    public String name;

    public Path path;

    public int index;

    public int blockCount;

    public MyBlock[] blocks;

    public int[][][] pageCounts = new int[Constant.THREAD_COUNT][Constant.PAGE_COUNT][Constant.MAX_COL_COUNT];

    public int dataCount;

    public AtomicInteger readCount = new AtomicInteger(0);

    public void initBlocks(int blockCount) {
        if (this.blockCount > 0) return;
        synchronized (this) {
            if (this.blockCount > 0) return;
            blocks = new MyBlock[blockCount];
            for (int i = 0; i < blockCount; ++i) {
                blocks[i] = new MyBlock();
            }
            this.blockCount = blockCount;
        }
    }
}

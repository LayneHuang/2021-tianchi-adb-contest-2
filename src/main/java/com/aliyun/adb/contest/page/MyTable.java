package com.aliyun.adb.contest.page;

public class MyTable {
    public int blockCount;

    public LineType lineType;

    public MyBlock[] blocks;

    public volatile int readCount;

    public MyTable(int blockCount) {
        blocks = new MyBlock[blockCount];
    }

    public synchronized void addReadCount() {
        readCount++;
    }
}

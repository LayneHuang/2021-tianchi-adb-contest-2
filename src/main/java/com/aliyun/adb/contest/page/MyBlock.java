package com.aliyun.adb.contest.page;

public class MyBlock {

    public int tableIndex;

    public int blockIndex;

    public long begin;

    public long end;

    public long getSize() {
        return end - begin + 1;
    }

    public int beginCur = 0;

    public byte[] beginBytes = new byte[40];

    public void addBeginByte(byte b) {
        beginBytes[beginCur++] = b;
    }

}

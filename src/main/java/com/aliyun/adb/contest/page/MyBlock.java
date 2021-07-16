package com.aliyun.adb.contest.page;

public class MyBlock {

    public int tableIndex;

    public int blockIndex;

    public long begin;

    public long end;

    public long getSize() {
        return end - begin + 1;
    }

    public boolean isD;

    public int lastColIndex;

    public long lastInput;

    public double lastInputD;

    public int firstNumLen = -1;

    public int beginLen;

    public long[] begins = new long[50];

}

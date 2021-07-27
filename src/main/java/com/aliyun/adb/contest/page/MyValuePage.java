package com.aliyun.adb.contest.page;

import com.aliyun.adb.contest.Constant;

public class MyValuePage {

    public long[] data = new long[Constant.WRITE_COUNT];

    public int size;

    public int dataCount;

    public void add(long value) {
        data[size++] = value;
        dataCount++;
    }

    public boolean full() {
        return size >= Constant.WRITE_COUNT;
    }

    public long[] copy() {
        if (size == 0) return null;
        long[] result = new long[size];
        System.arraycopy(data, 0, result, 0, size);
        return result;
    }
}

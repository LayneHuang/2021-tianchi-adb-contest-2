package com.aliyun.adb.contest.page;

import com.aliyun.adb.contest.Constant;

public class MyValuePage extends MyPage {

    public long[] data = new long[Constant.WRITE_COUNT];

    @Override
    public void add(long value) {
        data[size++] = value;
        dataCount++;
    }

    @Override
    public boolean full() {
        return size >= Constant.WRITE_COUNT;
    }

    @Override
    public void clean() {
        size = 0;
    }

    @Override
    public Object getData() {
        return data;
    }

    public long[] copy() {
        if (size == 0) return null;
        long[] result = new long[size];
        System.arraycopy(data, 0, result, 0, size);
        return result;
    }
}

package com.aliyun.adb.contest.page;

import com.aliyun.adb.contest.Constant;

import java.nio.ByteBuffer;

public class MyBufferPage extends MyPage {

    public ByteBuffer buffer = ByteBuffer.allocate(Constant.WRITE_SIZE);

    @Override
    public void add(long value) {
        buffer.putLong(value);
        size++;
        dataCount++;
    }

    @Override
    public boolean full() {
        return !buffer.hasRemaining();
    }

    @Override
    public void clean() {
        size = 0;
        buffer.clear();
    }

    @Override
    public Object getData() {
        return buffer;
    }

}

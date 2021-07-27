package com.aliyun.adb.contest.page;

import com.aliyun.adb.contest.Constant;

import java.nio.ByteBuffer;

public class MyValuePage {

    public ByteBuffer byteBuffer;

    public int dataCount;

    public void add(long value) {
        if (byteBuffer == null) byteBuffer = ByteBuffer.allocate(Constant.WRITE_SIZE);
        byteBuffer.putLong(value);
        dataCount++;
    }

}

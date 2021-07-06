package com.aliyun.adb.contest.page;

import com.aliyun.adb.contest.Constant;

import java.nio.ByteBuffer;

public class MyValuePage {

    public int tableIndex;

    public int columnIndex;

    public int blockIndex;

    public int pageIndex;

    public ByteBuffer byteBuffer;

    public void add(long value) {
        if (byteBuffer == null) {
            byteBuffer = ByteBuffer.allocate(Constant.WRITE_SIZE);
        }
        byteBuffer.putLong(value);
    }

}
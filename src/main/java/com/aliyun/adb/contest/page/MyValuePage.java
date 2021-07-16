package com.aliyun.adb.contest.page;

import com.aliyun.adb.contest.Constant;

import java.nio.ByteBuffer;

public class MyValuePage {

    /**
     * 第几张表
     */
    public int tableIndex;
    /**
     * 第几列
     */
    public int columnIndex;
    /**
     * 读入 这张表的 第几快
     */
    public int blockIndex;

    /**
     * 这一块中的某一页 [0,distance), [distance,2*distance-1), ...[ , Long.Max)
     */
    public int pageIndex;

    public ByteBuffer byteBuffer;

    public int dataCount;

    public void add(long value) {
        if (byteBuffer == null) {
            byteBuffer = ByteBuffer.allocate(Constant.WRITE_SIZE);
        }
        byteBuffer.putLong(value);
        dataCount++;
    }

}

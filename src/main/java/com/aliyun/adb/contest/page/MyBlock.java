package com.aliyun.adb.contest.page;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.nio.MappedByteBuffer;

public class MyBlock {

    public int tableIndex;

    public int blockIndex;

    public long begin;

    public long end;

    public long getSize() {
        return end - begin + 1;
    }

    private int beginCur = 0;

    public byte[] leftBegin = new byte[40];

    /**
     * 数据最长位数, 区分 int, long
     */
    private int maxDataLen;

    public void trans(MappedByteBuffer buffer) {
        byte b;
        if (blockIndex == 0) {
            while ((b = buffer.get()) != 10) {
                // 去除文件头的列名
                if (b == 44) {
                }
            }
            // Todo: 根据取的列做初始化
        } else {
            while (buffer.hasRemaining()) {
                b = buffer.get();
                if (b == 10) {
                    break;
                }
                leftBegin[beginCur++] = b;
            }
        }
        while (buffer.hasRemaining()) {
            handleByte(buffer.get());
        }
        unmap(buffer);
    }

    /**
     * 整数位
     */
    private long input;
    private int inputIndex;

    private void handleByte(byte b) {
        if (b >= 48) {
            input = input * 10 + b - 48;
            maxDataLen++;
        } else if (b == 46) {
            // 小数点
        } else {
            putData();
            maxDataLen = 0;
            inputIndex = (b == 44) ? 1 : 0;
            input = 0;
        }
    }

    private void putData() {
        putLongs();
    }

    private void putLongs() {

    }

    private static void unmap(MappedByteBuffer bb) {
        Cleaner cl = ((DirectBuffer) bb).cleaner();
        if (cl != null) {
            cl.clean();
        }
    }

}

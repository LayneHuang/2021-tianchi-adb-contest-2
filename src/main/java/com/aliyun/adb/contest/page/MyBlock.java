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

    public long[] dataLong;

    public int[] dataInt;

    public double[] dataDouble;

    private int beginCur = 0;

    private int endCur = 0;

    public byte[] leftBegin = new byte[40];

    public byte[] leftEnd = new byte[40];

    public void trans(MappedByteBuffer buffer) {
        if (blockIndex == 0) {
            while (buffer.get() != 10) {
                // 去除文件头的列名
            }
        } else {
            while (buffer.hasRemaining()) {
                byte b = buffer.get();
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

    private void handleByte(byte b) {

    }

    private static void unmap(MappedByteBuffer bb) {
        Cleaner cl = ((DirectBuffer) bb).cleaner();
        if (cl != null) {
            cl.clean();
        }
    }

}

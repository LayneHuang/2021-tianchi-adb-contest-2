package com.aliyun.adb.contest;


import com.aliyun.adb.contest.page.MyMemoryPage;
import com.aliyun.adb.contest.page.MyPage;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.nio.MappedByteBuffer;

public final class MyFileWriter extends Thread {

    private final MyFileReader reader;
    private final int threadIndex;
    private final int pageCount;
    private final long currentBlockCount;
    private final long pageSize;
    public MyPage[][] pages;

    public MyFileWriter(MyFileReader reader) {
        this.reader = reader;
        this.threadIndex = reader.threadIndex;
        this.pageCount = reader.pageCount;
        this.currentBlockCount = reader.currentBlockCount;
        this.pageSize = Long.MAX_VALUE / pageCount;

        pages = new MyPage[2][pageCount];
        for (int i = 0; i < pageCount; i++) {
            long start, end;
            start = (Long.MAX_VALUE / pageCount) * i;
            if (i == pageCount - 1) {
                end = Long.MAX_VALUE;
            } else {
                end = (Long.MAX_VALUE / pageCount) * (i + 1) - 1;
            }
            // 偷鸡做法 putLongs时判断
            pages[0][i] = new MyMemoryPage(start, end);
            pages[1][i] = new MyMemoryPage(start, end);
        }
    }

    private int getIndex(long value) {
        int index = (int) (value / pageSize);
        if (index == pageCount) {
            index--;
        }
        return index;
    }

    private void putLongs() {
        if (input == 0) {
            return;
        }
        int index = getIndex(input);
        pages[inputIndex][index].add(input);
    }

    private final byte[] preBytes = new byte[40];
    private int preBytesLen = 0;
    public MyFileWriter nextMyFileWriter;

    private void flush() {
        if (nextMyFileWriter != null) {
            for (int i = 0; i < nextMyFileWriter.preBytesLen; i++) {
                handleByte(nextMyFileWriter.preBytes[i]);
            }
        }
        if (inputIndex != 0) {
            putLongs();
        }
    }

    private long input;
    private int inputIndex = 0;

    private void handleByte(byte b) {
        if (b >= 48) {
            input = input * 10 + b - 48;
        } else {
            putLongs();
            inputIndex = (b == 44) ? 1 : 0;
            input = 0;
        }
    }

    @Override
    public void run() {
        try {
            long t = System.currentTimeMillis();
            for (int i = 0; i < currentBlockCount; i++) {
                MappedByteBuffer buffer = reader.myCache.poll();
                if (i == 0) {
                    if (threadIndex == 0) {
                        while (buffer.get() != 10) {
                            // 去除文件头的列名
                        }
                    } else {
                        // 获取开头不完整的部分
                        while (buffer.hasRemaining()) {
                            byte b = buffer.get();
                            if (b == 10) {
                                break;
                            }
                            preBytes[preBytesLen++] = b;
                        }
                    }
                }

                while (buffer.hasRemaining()) {
                    handleByte(buffer.get());
                }
                unmap(buffer);
            }
            flush();
            System.out.println("Thread(" + threadIndex + ") cost: (" + (System.currentTimeMillis() - t) + ")ms");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void unmap(MappedByteBuffer bb) {
        Cleaner cl = ((DirectBuffer) bb).cleaner();
        if (cl != null) {
            cl.clean();
        }
    }
}

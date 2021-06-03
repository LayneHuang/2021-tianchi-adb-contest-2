package com.aliyun.adb.contest;


import com.aliyun.adb.contest.page.MyMemoryPage;
import com.aliyun.adb.contest.page.MyPage;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.nio.MappedByteBuffer;

public class MyFileWriter extends Thread {

    private MyFileReader reader;
    private int threadIndex;
    private int pageCount;
    private long currentBlockCount;
    public MyPage[][] pages;

    public MyFileWriter(MyFileReader reader) {
        this.reader = reader;
        this.threadIndex = reader.threadIndex;
        this.pageCount = reader.pageCount;
        this.currentBlockCount = reader.currentBlockCount;

        pages = new MyPage[2][pageCount];
        for (int i = 0; i < pageCount; i++) {
            long start, end;
            start = (Long.MAX_VALUE / pageCount) * i;
            if (i == pageCount - 1) {
                end = Long.MAX_VALUE;
            } else {
                end = (Long.MAX_VALUE / pageCount) * (i + 1) - 1;
            }
            // 不偷鸡做法
//            pages[1][i] = new MyMemoryPage(start, end);
//            pages[1][i] = new MyFilePage(start, end, threadIndex);

            // PAGE_COUNT = 1000 的偷鸡做法 使用MyFakePage
//            if (i == 0 || i % 10 == 9) {
//                pages[0][i] = new MyMemoryPage(start, end);
//                pages[1][i] = new MyMemoryPage(start, end);
//            } else {
//                pages[0][i] = new MyFakePage(start, end);
//                pages[1][i] = new MyFakePage(start, end);
//            }

            // PAGE_COUNT = 1000 的偷鸡做法 使用isFake标记
//            pages[0][i] = new MyMemoryPage(start, end);
//            pages[1][i] = new MyMemoryPage(start, end);
//            if (i != 0 && i % 10 != 9) {
//                pages[0][i].isFake = true;
//                pages[1][i].isFake = true;
//            }

            // 偷鸡做法 putLongs时判断
            pages[0][i] = new MyMemoryPage(start, end);
            pages[1][i] = new MyMemoryPage(start, end);
        }
    }

    private int getIndex(long value) {
        int index = (int) (value / (Long.MAX_VALUE / pageCount));
        if (index == pageCount) {
            index--;
        }
        return index;
    }

//    private void putLongs(long[] input) {
//        pages[0][getIndex(input[0])].add(input[0]);
//        pages[1][getIndex(input[1])].add(input[1]);
//    }

    // PAGE_COUNT = 1000 的偷鸡做法 不使用MyFakePage和isFake标记
    private void putLongs(long[] input) {
        int index1 = getIndex(input[0]);
        if (index1 != 0) {
            if (index1 % 10 != 9) {
                pages[0][index1].size++;
            } else {
                pages[0][index1].add(input[0]);
            }
        } else {
            pages[0][index1].setMinValue(input[0]);
        }

        int index2 = getIndex(input[1]);
        if (index2 != 0) {
            if (index2 % 10 != 9) {
                pages[1][index2].size++;
            } else {
                pages[1][index2].add(input[1]);
            }
        } else {
            pages[1][index2].setMinValue(input[1]);
        }
    }

    private byte[] preBytes = new byte[40];
    private int preBytesLen = 0;
    public MyFileWriter nextMyFileWriter;

    private void flush() {
        if (nextMyFileWriter != null) {
            for (int i = 0; i < nextMyFileWriter.preBytesLen; i++) {
                handleByte(nextMyFileWriter.preBytes[i]);
            }
        }
        if (inputIndex != 0) {
            putLongs(inputs);
        }
    }

    private long[] inputs = {0L, 0L};
    private int inputIndex = 0;

    private void handleByte(byte b) {
        if (b >= 48) {
            inputs[inputIndex] = inputs[inputIndex] * 10 + b - 48;
        } else if (b == 44) {
            inputIndex = (inputIndex + 1) % 2;
        } else {
            putLongs(inputs);
            inputIndex = 0;
            inputs[0] = 0;
            inputs[1] = 0;
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
            System.out.println(threadIndex + " " + (System.currentTimeMillis() - t));
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
